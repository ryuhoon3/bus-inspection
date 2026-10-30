"""버스 단말기 현장 가이드 API.

실행: python main.py
접속: http://127.0.0.1:8000
"""

from __future__ import annotations

import json
import logging
import os
import re
import socket
import sqlite3
import sys
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime
from pathlib import Path
from typing import Iterator, Optional

for _stream in (sys.stdout, sys.stderr):
    if hasattr(_stream, "reconfigure"):
        try:
            _stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, Response
from pydantic import BaseModel, model_validator

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
DB_PATH = DATA_DIR / "fieldguide.db"
INDEX_PATH = ROOT / "index.html"
MIN_SCORE = 30

load_dotenv(ROOT / ".env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("fieldguide")

SCHEMA = """
CREATE TABLE IF NOT EXISTS models (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    code TEXT NOT NULL DEFAULT '',
    note TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS manuals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    model_id INTEGER NOT NULL,
    error_code TEXT NOT NULL,
    keywords TEXT NOT NULL DEFAULT '',
    title TEXT NOT NULL,
    cause TEXT NOT NULL,
    inspection TEXT NOT NULL,
    action TEXT NOT NULL,
    old_branch TEXT NOT NULL DEFAULT '',
    new_branch TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL,
    FOREIGN KEY (model_id) REFERENCES models(id) ON DELETE CASCADE,
    UNIQUE (model_id, error_code)
);

CREATE TABLE IF NOT EXISTS history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    model_id INTEGER,
    model_name TEXT NOT NULL DEFAULT '',
    query TEXT NOT NULL,
    cause TEXT NOT NULL,
    inspection TEXT NOT NULL,
    action TEXT NOT NULL,
    source TEXT NOT NULL,
    matched_code TEXT NOT NULL DEFAULT '',
    matched_title TEXT NOT NULL DEFAULT '',
    manual_id INTEGER,
    notice TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_manuals_model ON manuals(model_id);
CREATE INDEX IF NOT EXISTS idx_history_created ON history(created_at);
"""

SEED_MODELS = [
    {
        "name": "통합단말기",
        "code": "INT",
        "note": "B800, B710, B700이 공통으로 사용하는 통합단말기입니다.",
    },
    {
        "name": "표출단말기",
        "code": "DISP",
        "note": "B800, B710, B700이 공통으로 사용하는 표출단말기입니다.",
    },
    {
        "name": "LTE모뎀",
        "code": "LTE",
        "note": "B800, B710, B700이 공통으로 사용하는 LTE모뎀입니다.",
    },
    {
        "name": "승하차단말기",
        "code": "GATE",
        "note": "B800과 B700이 같은 승하차단말기를 사용합니다. B710에는 쓰지 않습니다.",
    },
    {
        "name": "B650",
        "code": "B650",
        "note": "B710 승하차단말기입니다. B800, B700 승하차단말기와 다른 기종입니다.",
    },
]

SYSTEMS = {
    "B800": "통합단말기, 표출단말기, LTE모뎀은 세 기종 공통입니다. 승하차단말기는 B700과 같은 기종입니다.",
    "B710": "통합단말기, 표출단말기, LTE모뎀은 공통입니다. 승하차단말기는 B650을 사용합니다.",
    "B700": "통합단말기, 표출단말기, LTE모뎀은 세 기종 공통입니다. 승하차단말기는 B800과 같은 기종입니다.",
}

DEVICE_DEFS = [
    {"code": "INT", "label": "통합단말기"},
    {"code": "DISP", "label": "표출단말기"},
    {"code": "LTE", "label": "LTE모뎀"},
    {"code": "GATE", "label": "승하차단말기"},
]
BUILTIN_MODEL_CODES = {"INT", "DISP", "LTE", "GATE", "B650"}

SEED_MANUALS = [
    {
        "model": "INT",
        "error_code": "E-101",
        "keywords": "카드,인식,태그,안찍,교통카드,통합",
        "title": "교통카드 인식 실패",
        "cause": "통합단말기 카드 인식부 문제이거나 카드 자체 불량입니다.",
        "inspection": "1. 다른 카드 2장으로 같은 위치에 태그합니다.\n2. 통합단말기 화면의 에러 코드를 기록합니다.\n3. 카드 인식부 케이블이 빠져 있는지만 확인합니다.",
        "old_branch": "B800, B710, B700 공통 통합단말기입니다. 차량이 달라도 같은 절차입니다.",
        "new_branch": "승하차단말기 매뉴얼과 섞지 않습니다. B710 승하차는 B650입니다.",
        "action": "다른 카드만 되면 카드 불량으로 안내합니다. 모두 실패하면 통합단말기 예비품 교체 대상입니다.",
    },
    {
        "model": "INT",
        "error_code": "E-415",
        "keywords": "SAM,인증,보안,모듈",
        "title": "SAM 인증 오류",
        "cause": "통합단말기 요금 SAM 인식 또는 인증 상태 문제입니다.",
        "inspection": "1. 미장착인지 인증실패인지 화면 문구를 적습니다.\n2. 다른 슬롯으로 옮기지 않습니다.\n3. 전원을 끄기 전에 하위 코드를 기록합니다.",
        "old_branch": "B800, B710, B700 공통 절차입니다.",
        "new_branch": "SAM은 통합단말기에만 있습니다. 승하차단말기 SAM으로 바꾸지 않습니다.",
        "action": "재인증 또는 재장착 후 테스트 승인 1건을 확인합니다. 같은 코드가 남으면 운행을 중단하고 일련번호를 남깁니다.",
    },
    {
        "model": "DISP",
        "error_code": "E-201",
        "keywords": "표출,화면,안나옴,검정,노선표시,디스플레이",
        "title": "표출 화면 무표시",
        "cause": "표출단말기 전원 또는 영상 케이블 접촉 불량입니다. 통합단말기가 꺼져 있으면 표출도 죽습니다.",
        "inspection": "1. 통합단말기 화면이 살아 있는지 먼저 확인합니다.\n2. 표출단말기 전원 램프를 확인합니다.\n3. 영상 케이블이 빠져 있으면 다시 체결합니다.",
        "old_branch": "B800, B710, B700 공통 표출단말기입니다.",
        "new_branch": "승하차 표시창 문제이면 표출단말기가 아니라 승하차단말기를 선택하세요.",
        "action": "케이블 재체결 후 노선이 나오면 정상입니다. 통합은 정상인데 표출만 꺼지면 표출단말기 예비품으로 교체합니다.",
    },
    {
        "model": "DISP",
        "error_code": "E-118",
        "keywords": "노선,정류장,표출,안내,불일치,화면내용",
        "title": "노선 표출 불일치",
        "cause": "표출 내용이 통합단말기 노선과 다릅니다. 표출 장치 고장과 노선 데이터 불일치를 구분해야 합니다.",
        "inspection": "1. 통합단말기 노선 번호와 표출 문구를 비교합니다.\n2. 표출만 오래됐으면 표출 수신을 한 번 갱신합니다.\n3. 글자가 깨지거나 잔상인지 확인합니다.",
        "old_branch": "B800, B710, B700 공통 표출단말기입니다.",
        "new_branch": "노선 데이터가 다르면 단말기를 교체하지 말고 노선 수신을 요청합니다.",
        "action": "갱신 후 다음 정류장 1곳이 맞으면 정상입니다. 글자 깨짐이 남으면 표출단말기 교체 대상입니다.",
    },
    {
        "model": "LTE",
        "error_code": "E-310",
        "keywords": "통신,LTE,모뎀,오프라인,망,인터넷,안테나",
        "title": "LTE 통신 두절",
        "cause": "LTE모뎀이 망에 붙지 않아 센터 통신이 되지 않습니다.",
        "inspection": "1. 모뎀 전원 램프와 안테나 체결을 확인합니다.\n2. 같은 장소의 다른 차량 통신과 비교합니다.\n3. 모뎀만 재접속하고 APN 등 설정값은 바꾸지 않습니다.",
        "old_branch": "B800, B710, B700 공통 LTE모뎀입니다.",
        "new_branch": "승하차단말기 통신 오류와 구분합니다. 센터 승인이 안 나가면 LTE모뎀입니다.",
        "action": "재접속이 실패하면 LTE모뎀 예비품으로 교체합니다. USIM을 다른 차량과 바꾸지 않습니다.",
    },
    {
        "model": "LTE",
        "error_code": "E-311",
        "keywords": "모뎀,전원,램프,꺼짐,LTE전원",
        "title": "LTE모뎀 전원 이상",
        "cause": "LTE모뎀 전원이 들어오지 않습니다. 통합단말기 전원과 모뎀 전원은 따로 확인합니다.",
        "inspection": "1. 통합단말기는 켜져 있는지 확인합니다.\n2. 모뎀 전원 커넥터 들뜸과 변색을 봅니다.\n3. 안테나 케이블을 억지로 당기지 않습니다.",
        "old_branch": "B800, B710, B700 공통 LTE모뎀입니다.",
        "new_branch": "모뎀만 꺼져 있으면 통합단말기 전체를 교체하지 않습니다.",
        "action": "커넥터 재체결 후 램프가 켜지는지 확인합니다. 안 켜지면 LTE모뎀 교체 대상입니다.",
    },
]


def int_field_manual(error_code: str, keywords: str, title: str, inspection: str, action: str) -> dict:
    """B800, B710 통합단말기 현장 증상. 기종은 공통이라 통합단말기 매뉴얼에 둔다."""
    return {
        "model": "INT",
        "error_code": error_code,
        "keywords": keywords,
        "title": title,
        "old_branch": "",
        "new_branch": "",
        "cause": f"{title} 처리입니다.",
        "inspection": inspection,
        "action": action,
    }


INT_FIELD_CODES = (
    "통합연결대기중",
    "BMS 불량",
    "펜소음 발생",
    "다인승 입력시 멈춤",
    "단인승 준비 실패",
)

SEED_MANUALS.extend(
    [
        int_field_manual(
            "통합연결대기중",
            "통합연결대기중,대기중,안넘어감,통신X",
            "통합연결대기중에서 안넘어감",
            "1. 승하차단말기에서 통신 X 상태일 경우 통합단말기 교체",
            "통합단말기 교체",
        ),
        int_field_manual(
            "BMS 불량",
            "BMS,T/B,TB,표출기",
            "BMS 불량",
            "1. 표출기에 T/B가 떠 있으면 통합단말기 BMS 불량으로 통합단말기 교체",
            "통합단말기 교체",
        ),
        int_field_manual(
            "펜소음 발생",
            "펜소음,팬소음,펜,팬,소음",
            "펜소음 발생",
            "1. 펜만 교체하여 수리 가능하나 점검시간이 오래걸림으로 통합단말기 교체",
            "통합단말기 교체",
        ),
        int_field_manual(
            "다인승 입력시 멈춤",
            "다인승,멈춤,입력",
            "다인승 입력시 멈춤",
            "1. 통합단말기 교체",
            "통합단말기 교체",
        ),
        int_field_manual(
            "단인승 준비 실패",
            "다인승,단인승,준비실패,문구",
            "다인승 입력시 단인승 준비 실패 문구",
            "1. 통합단말기 교체",
            "통합단말기 교체",
        ),
    ]
)


def lte_field_manual(error_code: str, keywords: str, title: str, inspection: str, action: str) -> dict:
    """B700, B710 LTE모뎀 현장 증상. B800 검색에서는 빼 둔다."""
    return {
        "model": "LTE",
        "error_code": error_code,
        "keywords": keywords,
        "title": title,
        "old_branch": "",
        "new_branch": "",
        "cause": f"B700, B710 {title} 처리입니다.",
        "inspection": inspection,
        "action": action,
    }


LTE_B700_B710_CODES = (
    "적색불",
    "ST1 미점등",
    "ST2 미점등",
)

SEED_MANUALS.extend(
    [
        lte_field_manual(
            "적색불",
            "적색불,적색,USIM,세척",
            "LTE모뎀 적색불",
            "1. USIM 세척 후 동일할 경우 LTE모뎀 교체",
            "LTE모뎀 교체",
        ),
        lte_field_manual(
            "ST1 미점등",
            "ST1,미점등",
            "LTE모뎀 ST1 미점등",
            "1. 통합단말기 불량으로 통합단말기 교체",
            "통합단말기 교체",
        ),
        lte_field_manual(
            "ST2 미점등",
            "ST2,미점등,USIM,세척",
            "LTE모뎀 ST2 미점등",
            "1. USIM 세척 후 동일할 경우 LTE모뎀 교체",
            "LTE모뎀 교체",
        ),
    ]
)


LTE_FAULT_CODE = "LTE모뎀 불량"

SEED_MANUALS.append(
    {
        "model": "LTE",
        "error_code": LTE_FAULT_CODE,
        "keywords": "LTE모뎀불량,적색불,미점등,USIM,G아이콘",
        "title": "LTE모뎀 불량",
        "old_branch": "",
        "new_branch": "",
        "cause": "LTE모뎀 불량 처리입니다.",
        "inspection": "\n".join(
            [
                "1. LTE모뎀 LTE에 적색불, 미점등시 USIM세척 해보고 증상동일하면 LTE모뎀 교체",
                "2. 표출단말기 우측 상단에 G아이콘 빼고 전부 비활성화 시 LTE모뎀 USIM 세척 후 증상 동일하면 LTE모뎀 교체",
            ]
        ),
        "action": "증상 동일하면 LTE모뎀 교체",
    }
)


B700_INT_CODES = ("신호등 및 속도 미표출",)

SEED_MANUALS.append(
    {
        "model": "INT",
        "error_code": "신호등 및 속도 미표출",
        "keywords": "신호등,속도,미표출,V2X,5G,아이콘",
        "title": "신호등 및 속도 미표출",
        "old_branch": "",
        "new_branch": "",
        "cause": "B700 신호등 및 속도 미표출 처리입니다.",
        "inspection": "\n".join(
            [
                "1. 표출단말기 상단 좌측 아이콘 C, 5G만 비활성화 시 외부 장비인 V2X 문제임으로 V2X 상황실 인계",
                "2. V2X 장비 5G, LTE 점등되어있는데 표출단말기 좌측 아이콘 전부 비활성화 경우 통합단말기 교체",
            ]
        ),
        "action": "C, 5G만 꺼지면 V2X 상황실 인계. 아이콘이 전부 꺼지면 통합단말기 교체",
    }
)


POWER_WHOLE_CODE = "단말기 전원불량"

SEED_MANUALS.extend(
    [
        {
            "model": model,
            "error_code": POWER_WHOLE_CODE,
            "keywords": "전체전원,표출전원,퓨즈,전원케이블,연동",
            "title": "단말기 전체 전원 불량",
            "old_branch": "",
            "new_branch": "",
            "cause": "단말기 전체 전원 불량 처리입니다.",
            "inspection": "\n".join(
                [
                    "1. 표출단말기, 통합단말기 전원이 연동되어 있으므로 먼저 표출 전원불량 여부 확인",
                    "2. 단말기 이상 없으면 차량 퓨즈 및 전원케이블 확인",
                ]
            ),
            "action": "단말기 이상 없으면 차량 퓨즈 및 전원케이블 확인",
        }
        for model in ("INT", "DISP")
    ]
)


def disp_field_manual(error_code: str, keywords: str, title: str, inspection: str) -> dict:
    """표출단말기 현장 증상. B800, B710, B700 공통이다."""
    return {
        "model": "DISP",
        "error_code": error_code,
        "keywords": keywords,
        "title": title,
        "old_branch": "",
        "new_branch": "",
        "cause": f"{title} 처리입니다.",
        "inspection": inspection,
        "action": "표출단말기 교체",
    }


DISP_FIELD_CODES = (
    "표출단말기 화면불량",
    "표출단말기 부팅불량",
    "표출단말기 리커버리모드",
    "표출단말기 음성불량",
)

SEED_MANUALS.extend(
    [
        disp_field_manual(
            "표출단말기 화면불량",
            "화면불량,하얀,하얗게,검은화면,흰화면,카드태그,화면안보임,백화,흑화,화면백화,화면흑화",
            "표출단말기 화면불량",
            "1. 승하차 단말기 카드 태그는 정상이나 화면이 하얗게 또는 검은화면만 보인다면 표출단말기 교체",
        ),
        disp_field_manual(
            "표출단말기 부팅불량",
            "부팅불량,부팅,상태표시,LED,붉은색,적색",
            "표출단말기 부팅불량",
            "1. 표출단말기 화면이 검은색으로만 나오며 상단 상태표시LED가 붉은색 2개 들어와있으면 표출단말기 교체",
        ),
        disp_field_manual(
            "표출단말기 리커버리모드",
            "리커버리,리커버리모드,안드로이드,영어,영어로,설정화면",
            "표출단말기 리커버리모드",
            "1. 표출단말기 부팅중 안드로이드 그림나오면서 영어로 설정 화면이 나오면 표출단말기 교체",
        ),
        disp_field_manual(
            "표출단말기 음성불량",
            "음성불량,음성,음성작음",
            "표출단말기 음성불량",
            "1. 음성이 작거나 안나오는 증상 발생 시 표출단말기 교체",
        ),
    ]
)


SEED_MANUALS.append(
    {
        "model": "INT",
        "error_code": "GPS장애",
        "keywords": "GPS,위치변경,안테나,간섭,타사,수신",
        "title": "GPS장애",
        "old_branch": "",
        "new_branch": "",
        "cause": "GPS장애 처리입니다.",
        "inspection": "\n".join(
            [
                "1. GPS 위치변경 해보기",
                "2. GPS 위치변경 후에도 증상동일하면 GPS 안테나 교체",
                "3. GPS 안테나 교체 후에도 증상동일할 경우 차량 내외부 타사 장비 간섭 찾기, 타사장비 1개씩 꺼보면서 GPS수신상태 점검하기",
            ]
        ),
        "action": "증상 같으면 다음 단계로 갑니다.",
    }
)


RUN_FIELD_CODES = ("무정차", "개문주행", "과속")

_GPS_RUN_STEPS = [
    "1. GPS수신감도 확인 후 GPS 위치변경",
    "2. GPS위치변경 후에도 증상동일하면 GPS 안테나 교체",
    "3. GPS 타사 장비 간섭여부 확인",
]

SEED_MANUALS.extend(
    [
        {
            "model": "INT",
            "error_code": "무정차",
            "keywords": "무정차",
            "title": "무정차",
            "old_branch": "",
            "new_branch": "",
            "cause": "무정차 처리입니다.",
            "inspection": "\n".join(_GPS_RUN_STEPS),
            "action": "증상 같으면 다음 단계로 갑니다.",
        },
        {
            "model": "INT",
            "error_code": "개문주행",
            "keywords": "개문주행,개문,DTG,앞문,뒷문",
            "title": "개문주행",
            "old_branch": "",
            "new_branch": "",
            "cause": "개문주행 처리입니다.",
            "inspection": "\n".join(
                [
                    "1. 차량 앞문뒷문 레버 조작 시 단말기 DTG상에 열림, 닫힘 정보 이상없는지 확인",
                    "2. 앞문 뒷문 DTG HIGH/LOW 설정 되어있는지 확인",
                ]
            ),
            "action": "앞문 뒷문 DTG HIGH/LOW 설정 되어있는지 확인",
        },
        {
            "model": "INT",
            "error_code": "과속",
            "keywords": "과속",
            "title": "과속",
            "old_branch": "",
            "new_branch": "",
            "cause": "과속 처리입니다.",
            "inspection": "\n".join(_GPS_RUN_STEPS),
            "action": "증상 같으면 다음 단계로 갑니다.",
        },
    ]
)


AHEAD_CODE = "앞뒤차 안보임"

SEED_MANUALS.extend(
    [
        {
            "model": model,
            "error_code": AHEAD_CODE,
            "keywords": "앞뒤차,안보임,아이콘,G만,T/B,TB,BMS,GPS,내차,앞차,뒤차",
            "title": "앞뒤차 안보임",
            "old_branch": "",
            "new_branch": "",
            "cause": "앞뒤차 안보임 처리입니다.",
            "inspection": "\n".join(
                [
                    "1. LTE모뎀 불량으로 우측 상단 아이콘 G만 빼고 전부 비활성화 되어있는 경우 LTE모뎀 교체",
                    "2. 우측 상단 T/B 떠있는 경우 통합단말기 BMS불량으로 통합단말기 교체",
                    "3. 우측 상단 G만 비활성화 되어있는 경우 GPS 점검",
                ]
            ),
            "action": "G만 남으면 LTE모뎀 교체. T/B면 통합단말기 교체. G만 꺼지면 GPS 점검",
        }
        for model in ("INT", "LTE")
    ]
)


UNSENT_CODE = "미전송파일"

SEED_MANUALS.extend(
    [
        {
            "model": model,
            "error_code": UNSENT_CODE,
            "keywords": "미전송파일,미전송,전송안됨",
            "title": "표출기 미전송파일",
            "old_branch": "",
            "new_branch": "",
            "cause": "표출기 미전송파일은 LTE 통신불량으로 남아 있습니다.",
            "inspection": "1. 표출기에 미전송파일이 남아있는 경우 LTE 통신불량으로 인해 남아 있는 것이므로 LTE가 해결되면 미전송파일도 통신됨",
            "action": "LTE가 해결되면 미전송파일도 통신됩니다.",
        }
        for model in ("DISP", "LTE")
    ]
)


def gate_comm_steps(include_os: bool) -> str:
    """OS적용과 SAM 세척·위치변경은 B800, B700 승하차만 해당한다. B710은 뺀다."""
    lines = ["1. 통합단말기 {위치}케이블 연결상태 확인"]
    number = 2
    if include_os:
        lines.append("2. B800, B700만 OS적용 및 SAM 세척 SAM위치변경 해보기. B710은 해당 없음")
        number = 3
    for step in (
        "통합단말기 {위치} 포트변경 해보기",
        "{위치} 단말기 바꿔보기",
        "케이블 가포설 후 통신상태 확인해보기",
    ):
        lines.append(f"{number}. {step}")
        number += 1
    return "\n".join(lines)


def gate_field_manuals(
    model: str,
    device_name: str,
    old_branch: str,
    new_branch: str,
    b800_history: bool = False,
) -> list[dict]:
    """승하차는 에러코드보다 현장에서 갈라 보는 장애 유형으로 안내한다."""
    shared = {"model": model, "old_branch": old_branch, "new_branch": new_branch}
    manuals = [
        {
            **shared,
            "error_code": "통신불량",
            "keywords": "통신,불량,케이블,연결,포트,SAM,세척,OS,위치변경,교체,가포설,포트변경,바꿔",
            "title": "통신불량",
            "old_branch": "",
            "new_branch": "",
            "cause": "통신불량을 해결한 방법입니다.",
            "inspection": gate_comm_steps(include_os=model == "GATE"),
            "action": "통신이 살아나면 다음 단계로 가지 않습니다.",
        },
        {
            **shared,
            "error_code": "전원불량",
            "keywords": "전원,불량,안켜짐,꺼짐,램프,인가,스위치",
            "title": "전원불량",
            "cause": "전원부터 구분합니다.",
            "inspection": "\n".join(
                [
                    "전원 스위치 ON/OFF 확인",
                    "램프·화면 꺼짐 → 전원 확인",
                    "케이블 빠짐 → 재체결",
                    "케이블 손상 → 케이블 교체",
                    "통합만 정상, 이 위치만 → 그 구간 전원",
                ]
            ),
            "action": "전원은 오고 통신만 없으면 통신불량.",
        },
        {
            **shared,
            "error_code": "화면불량",
            "keywords": "화면,불량,안나옴,검정,깨짐,표시,LCD,교체",
            "title": "화면불량",
            "old_branch": "",
            "new_branch": "",
            "cause": "화면불량 처리입니다.",
            "inspection": "\n".join(
                [
                    "1. 단말기 교체로 처리하는 경우가 대부분",
                    "2. LCD 교체로도 해결 가능",
                ]
            ),
            "action": "단말기 교체",
        },
        {
            **shared,
            "error_code": "SAM통신오류",
            "keywords": "SAM,통신,오류,인증,모듈",
            "title": "SAM통신오류",
            "cause": "단말기 통신불량과 다른 증상입니다.",
            "inspection": "\n".join(
                [
                    "통합까지 통신 안 됨 → 통신불량",
                    "SAM만 실패 → 장착·접촉 확인",
                    "이 위치만 → 그 단말기 SAM",
                ]
            ),
            "action": "다른 기종 SAM과 바꾸지 않습니다.",
        },
        {
            **shared,
            "error_code": "PSAM 초기화 실패",
            "keywords": "PSAM,초기화,실패,세척,재삽입,교체,PSAM오류,SAM오류",
            "title": "PSAM 초기화 실패",
            "old_branch": "",
            "new_branch": "",
            "cause": "PSAM 초기화 실패 처리입니다.",
            "inspection": "\n".join(
                [
                    "1. SAM 세척 후 재삽입 조치",
                    "2. 세척 후 증상동일하면 단말기 교체",
                ]
            ),
            "action": "세척 후 증상동일하면 단말기 교체",
        },
        {
            **shared,
            "error_code": "부팅불량",
            "keywords": "승하차부팅,T머니,티머니,로고,부팅중",
            "title": "승하차 부팅불량",
            "old_branch": "",
            "new_branch": "",
            "cause": "승하차 부팅불량 처리입니다.",
            "inspection": "\n".join(
                [
                    "1. 통신상태 확인하기. 통신상태 정상이면 화면불량. 전원 스위치가 내려간 상태일 수도 있으니 전원 스위치 점검",
                    "2. 부팅중 T머니 로고에서 멈춤: 승하차 단말기 교체",
                    "3. 부팅중 검은 화면에서 진행안됨: 승하차 단말기 교체",
                ]
            ),
            "action": "T머니 로고에서 멈추거나 검은 화면에서 진행이 안 되면 승하차 단말기 교체",
        },
        {
            **shared,
            "error_code": "부저음",
            "keywords": "부저음,부저",
            "title": "승하차 단말기 부저음 안나옴",
            "old_branch": "",
            "new_branch": "",
            "cause": "승하차 단말기 부저음 안나옴 처리입니다.",
            "inspection": "1. 승하차 단말기 부저음 안나옴: 승하차 단말기 교체",
            "action": "승하차 단말기 교체",
        },
    ]
    if b800_history:
        apply_b800_gate_history(manuals, shared)
    return manuals


def apply_b800_gate_history(manuals: list[dict], shared: dict) -> None:
    """B800 승하차 처리내역. B700은 같은 기종이라 같은 조치를 쓰고, B650에는 넣지 않는다."""
    manuals.append(
        {
            **shared,
            "error_code": "카드무감",
            "keywords": "카드,무감,태그,안찍힘,인식안됨,SAM,세척,OS,위치변경,교체",
            "title": "카드무감",
            "old_branch": "",
            "new_branch": "",
            "cause": "카드무감 처리입니다.",
            "inspection": "\n".join(
                [
                    "1. B800의 경우 SAM 세척 및 SAM 위치변경 OS적용",
                    "2. 단말기 교체",
                ]
            ),
            "action": "단말기 교체",
        }
    )


SEED_MANUALS.extend(
    gate_field_manuals(
        "GATE",
        "B800, B700 공통 승하차단말기",
        "B800·B700 공통",
        "B710은 B650. 예비품 혼용 금지",
        b800_history=True,
    )
)
SEED_MANUALS.extend(
    gate_field_manuals(
        "B650",
        "B710 승하차단말기 B650",
        "B710 B650 전용",
        "B800·B700 예비품과 바꾸지 않음",
    )
)


def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def normalize(text: str) -> str:
    value = (text or "").lower()
    value = re.sub(r"\s+", "", value)
    return value.replace("-", "").replace("_", "")


def like_pattern(text: str) -> str:
    escaped = text.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return f"%{escaped}%"


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 5000")
    return conn


@contextmanager
def db() -> Iterator[sqlite3.Connection]:
    conn = get_conn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def seed(conn: sqlite3.Connection) -> None:
    code_to_id: dict[str, int] = {}
    for model in SEED_MODELS:
        cur = conn.execute(
            "INSERT INTO models (name, code, note) VALUES (?, ?, ?)",
            (model["name"], model["code"], model["note"]),
        )
        code_to_id[model["code"]] = int(cur.lastrowid)
    stamped = now_text()
    for item in SEED_MANUALS:
        conn.execute(
            """
            INSERT INTO manuals (
                model_id, error_code, keywords, title, cause, inspection,
                action, old_branch, new_branch, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                code_to_id[item["model"]],
                item["error_code"],
                item["keywords"],
                item["title"],
                item["cause"],
                item["inspection"],
                item["action"],
                item["old_branch"],
                item["new_branch"],
                stamped,
            ),
        )


def ensure_column(conn: sqlite3.Connection, table: str, column: str, definition: str) -> None:
    columns = {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")}
    if column not in columns:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def init_db() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with db() as conn:
        conn.executescript(SCHEMA)
        ensure_column(conn, "history", "system_code", "TEXT NOT NULL DEFAULT ''")
        ensure_column(conn, "history", "position_code", "TEXT NOT NULL DEFAULT ''")
        count = conn.execute("SELECT COUNT(*) AS c FROM models").fetchone()["c"]
        if count == 0:
            seed(conn)
            logger.info("B800, B710, B700 장치 매뉴얼을 넣었습니다.")
        else:
            replace_sample_catalog(conn)
        sync_gate_field_checks(conn)
        sync_b800_gate_history(conn)
        sync_gate_comm_vehicle(conn)
        sync_gate_boot_fault(conn)
        sync_gate_buzzer(conn)
        sync_gate_power_switch(conn)
        sync_psam_init_fail(conn)
        sync_card_blind(conn)
        sync_screen_fault(conn)
        sync_int_field_symptoms(conn)
        sync_lte_field_symptoms(conn)
        sync_lte_fault(conn)
        sync_b700_signal_speed(conn)
        sync_whole_power(conn)
        sync_disp_field_symptoms(conn)
        sync_gps_fault(conn)
        sync_run_field_symptoms(conn)
        sync_ahead_blind(conn)
        sync_unsent_files(conn)
        remove_receipt_manual(conn)


def sync_gate_field_checks(conn: sqlite3.Connection) -> None:
    """이미 만들어진 DB의 승하차 항목을 에러코드형에서 현장확인형으로 바꿉니다."""
    stamped = now_text()
    changed = False
    for code in ("GATE", "B650"):
        model = conn.execute("SELECT id FROM models WHERE code = ?", (code,)).fetchone()
        if not model:
            continue
        model_id = int(model["id"])
        exists = conn.execute(
            "SELECT id FROM manuals WHERE model_id = ? AND error_code = ?",
            (model_id, "통신불량"),
        ).fetchone()
        if exists:
            continue
        conn.execute(
            "DELETE FROM manuals WHERE model_id = ? AND error_code IN ('E-101', 'E-310')",
            (model_id,),
        )
        for item in SEED_MANUALS:
            if item["model"] != code:
                continue
            conn.execute(
                """
                INSERT INTO manuals (
                    model_id, error_code, keywords, title, cause, inspection,
                    action, old_branch, new_branch, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    model_id,
                    item["error_code"],
                    item["keywords"],
                    item["title"],
                    item["cause"],
                    item["inspection"],
                    item["action"],
                    item["old_branch"],
                    item["new_branch"],
                    stamped,
                ),
            )
        changed = True
    if changed:
        logger.info("승하차 장애를 현장확인 항목으로 바꿨습니다.")


def sync_psam_init_fail(conn: sqlite3.Connection) -> None:
    """승하차 PSAM 초기화 실패는 세척 후 재삽입, 같으면 단말기 교체만 안내한다."""
    stamped = now_text()
    changed = False
    for code in ("GATE", "B650"):
        model = conn.execute("SELECT id FROM models WHERE code = ?", (code,)).fetchone()
        if not model:
            continue
        item = next(
            (
                manual
                for manual in SEED_MANUALS
                if manual["model"] == code and manual["error_code"] == "PSAM 초기화 실패"
            ),
            None,
        )
        if not item:
            continue
        model_id = int(model["id"])
        row = conn.execute(
            "SELECT id, inspection, keywords FROM manuals WHERE model_id = ? AND error_code = ?",
            (model_id, item["error_code"]),
        ).fetchone()
        if row and "재삽입" in (row["inspection"] or "") and "SAM오류" in (row["keywords"] or ""):
            continue
        fields = (
            item["keywords"],
            item["title"],
            item["cause"],
            item["inspection"],
            item["action"],
            item["old_branch"],
            item["new_branch"],
            stamped,
        )
        if row:
            conn.execute(
                """
                UPDATE manuals
                SET keywords = ?, title = ?, cause = ?, inspection = ?, action = ?,
                    old_branch = ?, new_branch = ?, updated_at = ?
                WHERE id = ?
                """,
                (*fields, int(row["id"])),
            )
        else:
            conn.execute(
                """
                INSERT INTO manuals (
                    model_id, error_code, keywords, title, cause, inspection,
                    action, old_branch, new_branch, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (model_id, item["error_code"], *fields),
            )
        changed = True
    if changed:
        logger.info("PSAM 초기화 실패 안내를 넣었습니다.")


def sync_card_blind(conn: sqlite3.Connection) -> None:
    """카드무감은 B800 승하차 기준으로 세척·위치변경·OS 후 단말기 교체만 안내한다."""
    stamped = now_text()
    item = next(
        (
            manual
            for manual in SEED_MANUALS
            if manual["model"] == "GATE" and manual["error_code"] == "카드무감"
        ),
        None,
    )
    model = conn.execute("SELECT id FROM models WHERE code = 'GATE'").fetchone()
    if not item or not model:
        return
    row = conn.execute(
        "SELECT id, inspection FROM manuals WHERE model_id = ? AND error_code = ?",
        (int(model["id"]), item["error_code"]),
    ).fetchone()
    if row and "B800의 경우" in (row["inspection"] or ""):
        return
    fields = (
        item["keywords"],
        item["title"],
        item["cause"],
        item["inspection"],
        item["action"],
        item["old_branch"],
        item["new_branch"],
        stamped,
    )
    if row:
        conn.execute(
            """
            UPDATE manuals
            SET keywords = ?, title = ?, cause = ?, inspection = ?, action = ?,
                old_branch = ?, new_branch = ?, updated_at = ?
            WHERE id = ?
            """,
            (*fields, int(row["id"])),
        )
    else:
        conn.execute(
            """
            INSERT INTO manuals (
                model_id, error_code, keywords, title, cause, inspection,
                action, old_branch, new_branch, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (int(model["id"]), item["error_code"], *fields),
        )
    logger.info("카드무감 안내를 B800 처리 순서로 바꿨습니다.")


def sync_screen_fault(conn: sqlite3.Connection) -> None:
    """승하차 화면불량은 단말기 교체가 대부분이고, LCD 교체로도 해결된다."""
    stamped = now_text()
    changed = False
    for code in ("GATE", "B650"):
        model = conn.execute("SELECT id FROM models WHERE code = ?", (code,)).fetchone()
        item = next(
            (
                manual
                for manual in SEED_MANUALS
                if manual["model"] == code and manual["error_code"] == "화면불량"
            ),
            None,
        )
        if not model or not item:
            continue
        row = conn.execute(
            "SELECT id, inspection FROM manuals WHERE model_id = ? AND error_code = ?",
            (int(model["id"]), item["error_code"]),
        ).fetchone()
        if row and "LCD" in (row["inspection"] or ""):
            continue
        fields = (
            item["keywords"],
            item["title"],
            item["cause"],
            item["inspection"],
            item["action"],
            item["old_branch"],
            item["new_branch"],
            stamped,
        )
        if row:
            conn.execute(
                """
                UPDATE manuals
                SET keywords = ?, title = ?, cause = ?, inspection = ?, action = ?,
                    old_branch = ?, new_branch = ?, updated_at = ?
                WHERE id = ?
                """,
                (*fields, int(row["id"])),
            )
        else:
            conn.execute(
                """
                INSERT INTO manuals (
                    model_id, error_code, keywords, title, cause, inspection,
                    action, old_branch, new_branch, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (int(model["id"]), item["error_code"], *fields),
            )
        changed = True
    if changed:
        logger.info("화면불량 안내를 단말기 교체 기준으로 바꿨습니다.")


def sync_int_field_symptoms(conn: sqlite3.Connection) -> None:
    """B800, B710 통합단말기 현장 증상을 공통 통합단말기 매뉴얼에 맞춘다."""
    stamped = now_text()
    model = conn.execute("SELECT id FROM models WHERE code = 'INT'").fetchone()
    if not model:
        return
    changed = False
    model_id = int(model["id"])
    for item in SEED_MANUALS:
        if item["model"] != "INT" or item["error_code"] not in INT_FIELD_CODES:
            continue
        row = conn.execute(
            "SELECT id, inspection FROM manuals WHERE model_id = ? AND error_code = ?",
            (model_id, item["error_code"]),
        ).fetchone()
        if row and (row["inspection"] or "") == item["inspection"]:
            continue
        fields = (
            item["keywords"],
            item["title"],
            item["cause"],
            item["inspection"],
            item["action"],
            item["old_branch"],
            item["new_branch"],
            stamped,
        )
        if row:
            conn.execute(
                """
                UPDATE manuals
                SET keywords = ?, title = ?, cause = ?, inspection = ?, action = ?,
                    old_branch = ?, new_branch = ?, updated_at = ?
                WHERE id = ?
                """,
                (*fields, int(row["id"])),
            )
        else:
            conn.execute(
                """
                INSERT INTO manuals (
                    model_id, error_code, keywords, title, cause, inspection,
                    action, old_branch, new_branch, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (model_id, item["error_code"], *fields),
            )
        changed = True
    if changed:
        logger.info("통합단말기 현장 증상 5건을 넣었습니다.")


def sync_lte_field_symptoms(conn: sqlite3.Connection) -> None:
    """B700, B710 LTE모뎀 적색불·ST1·ST2 안내를 넣는다."""
    stamped = now_text()
    model = conn.execute("SELECT id FROM models WHERE code = 'LTE'").fetchone()
    if not model:
        return
    changed = False
    model_id = int(model["id"])
    for item in SEED_MANUALS:
        if item["model"] != "LTE" or item["error_code"] not in LTE_B700_B710_CODES:
            continue
        row = conn.execute(
            "SELECT id, inspection FROM manuals WHERE model_id = ? AND error_code = ?",
            (model_id, item["error_code"]),
        ).fetchone()
        if row and (row["inspection"] or "") == item["inspection"]:
            continue
        fields = (
            item["keywords"],
            item["title"],
            item["cause"],
            item["inspection"],
            item["action"],
            item["old_branch"],
            item["new_branch"],
            stamped,
        )
        if row:
            conn.execute(
                """
                UPDATE manuals
                SET keywords = ?, title = ?, cause = ?, inspection = ?, action = ?,
                    old_branch = ?, new_branch = ?, updated_at = ?
                WHERE id = ?
                """,
                (*fields, int(row["id"])),
            )
        else:
            conn.execute(
                """
                INSERT INTO manuals (
                    model_id, error_code, keywords, title, cause, inspection,
                    action, old_branch, new_branch, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (model_id, item["error_code"], *fields),
            )
        changed = True
    if changed:
        logger.info("B700, B710 LTE모뎀 안내 3건을 넣었습니다.")


def sync_lte_fault(conn: sqlite3.Connection) -> None:
    """LTE모뎀 불량은 적색불·미점등과 G아이콘만 남은 경우를 USIM 세척 후 교체로 안내한다."""
    stamped = now_text()
    item = next((manual for manual in SEED_MANUALS if manual["error_code"] == LTE_FAULT_CODE), None)
    model = conn.execute("SELECT id FROM models WHERE code = 'LTE'").fetchone()
    if not item or not model:
        return
    row = conn.execute(
        "SELECT id, inspection FROM manuals WHERE model_id = ? AND error_code = ?",
        (int(model["id"]), LTE_FAULT_CODE),
    ).fetchone()
    if row and (row["inspection"] or "") == item["inspection"]:
        return
    fields = (
        item["keywords"],
        item["title"],
        item["cause"],
        item["inspection"],
        item["action"],
        item["old_branch"],
        item["new_branch"],
        stamped,
    )
    if row:
        conn.execute(
            """
            UPDATE manuals
            SET keywords = ?, title = ?, cause = ?, inspection = ?, action = ?,
                old_branch = ?, new_branch = ?, updated_at = ?
            WHERE id = ?
            """,
            (*fields, int(row["id"])),
        )
    else:
        conn.execute(
            """
            INSERT INTO manuals (
                model_id, error_code, keywords, title, cause, inspection,
                action, old_branch, new_branch, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (int(model["id"]), LTE_FAULT_CODE, *fields),
        )
    logger.info("LTE모뎀 불량 안내를 넣었습니다.")


def sync_b700_signal_speed(conn: sqlite3.Connection) -> None:
    """B700 통합단말기의 신호등·속도 미표출은 아이콘 상태에 따라 둘로 가른다."""
    stamped = now_text()
    model = conn.execute("SELECT id FROM models WHERE code = 'INT'").fetchone()
    item = next(
        (
            manual
            for manual in SEED_MANUALS
            if manual["model"] == "INT" and manual["error_code"] in B700_INT_CODES
        ),
        None,
    )
    if not model or not item:
        return
    row = conn.execute(
        "SELECT id, inspection FROM manuals WHERE model_id = ? AND error_code = ?",
        (int(model["id"]), item["error_code"]),
    ).fetchone()
    if row and (row["inspection"] or "") == item["inspection"]:
        return
    fields = (
        item["keywords"],
        item["title"],
        item["cause"],
        item["inspection"],
        item["action"],
        item["old_branch"],
        item["new_branch"],
        stamped,
    )
    if row:
        conn.execute(
            """
            UPDATE manuals
            SET keywords = ?, title = ?, cause = ?, inspection = ?, action = ?,
                old_branch = ?, new_branch = ?, updated_at = ?
            WHERE id = ?
            """,
            (*fields, int(row["id"])),
        )
    else:
        conn.execute(
            """
            INSERT INTO manuals (
                model_id, error_code, keywords, title, cause, inspection,
                action, old_branch, new_branch, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (int(model["id"]), item["error_code"], *fields),
        )
    logger.info("B700 신호등 및 속도 미표출 안내를 넣었습니다.")


def sync_whole_power(conn: sqlite3.Connection) -> None:
    """표출과 통합의 전체 전원 불량은 두 장치 매뉴얼에 같은 문장으로 둔다."""
    stamped = now_text()
    item = next(
        (manual for manual in SEED_MANUALS if manual["error_code"] == POWER_WHOLE_CODE),
        None,
    )
    if not item:
        return
    changed = False
    for code in ("INT", "DISP"):
        model = conn.execute("SELECT id FROM models WHERE code = ?", (code,)).fetchone()
        if not model:
            continue
        row = conn.execute(
            "SELECT id, inspection FROM manuals WHERE model_id = ? AND error_code = ?",
            (int(model["id"]), POWER_WHOLE_CODE),
        ).fetchone()
        if row and (row["inspection"] or "") == item["inspection"]:
            continue
        fields = (
            item["keywords"],
            item["title"],
            item["cause"],
            item["inspection"],
            item["action"],
            item["old_branch"],
            item["new_branch"],
            stamped,
        )
        if row:
            conn.execute(
                """
                UPDATE manuals
                SET keywords = ?, title = ?, cause = ?, inspection = ?, action = ?,
                    old_branch = ?, new_branch = ?, updated_at = ?
                WHERE id = ?
                """,
                (*fields, int(row["id"])),
            )
        else:
            conn.execute(
                """
                INSERT INTO manuals (
                    model_id, error_code, keywords, title, cause, inspection,
                    action, old_branch, new_branch, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (int(model["id"]), POWER_WHOLE_CODE, *fields),
            )
        changed = True
    if changed:
        logger.info("단말기 전체 전원 불량 안내를 넣었습니다.")


def sync_disp_field_symptoms(conn: sqlite3.Connection) -> None:
    """표출단말기 화면불량, 부팅불량, 리커버리모드를 넣는다."""
    stamped = now_text()
    model = conn.execute("SELECT id FROM models WHERE code = 'DISP'").fetchone()
    if not model:
        return
    changed = False
    model_id = int(model["id"])
    for item in SEED_MANUALS:
        if item["model"] != "DISP" or item["error_code"] not in DISP_FIELD_CODES:
            continue
        row = conn.execute(
            "SELECT id, inspection, keywords FROM manuals WHERE model_id = ? AND error_code = ?",
            (model_id, item["error_code"]),
        ).fetchone()
        if (
            row
            and (row["inspection"] or "") == item["inspection"]
            and (row["keywords"] or "") == item["keywords"]
        ):
            continue
        fields = (
            item["keywords"],
            item["title"],
            item["cause"],
            item["inspection"],
            item["action"],
            item["old_branch"],
            item["new_branch"],
            stamped,
        )
        if row:
            conn.execute(
                """
                UPDATE manuals
                SET keywords = ?, title = ?, cause = ?, inspection = ?, action = ?,
                    old_branch = ?, new_branch = ?, updated_at = ?
                WHERE id = ?
                """,
                (*fields, int(row["id"])),
            )
        else:
            conn.execute(
                """
                INSERT INTO manuals (
                    model_id, error_code, keywords, title, cause, inspection,
                    action, old_branch, new_branch, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (model_id, item["error_code"], *fields),
            )
        changed = True
    if changed:
        logger.info("표출단말기 장애 3건을 넣었습니다.")


def sync_gps_fault(conn: sqlite3.Connection) -> None:
    """GPS장애는 위치변경, 안테나 교체, 타사 장비 간섭 확인 순서로 안내한다."""
    stamped = now_text()
    model = conn.execute("SELECT id FROM models WHERE code = 'INT'").fetchone()
    item = next((manual for manual in SEED_MANUALS if manual["error_code"] == "GPS장애"), None)
    if not model or not item:
        return
    row = conn.execute(
        "SELECT id, inspection FROM manuals WHERE model_id = ? AND error_code = ?",
        (int(model["id"]), item["error_code"]),
    ).fetchone()
    if row and (row["inspection"] or "") == item["inspection"]:
        return
    fields = (
        item["keywords"],
        item["title"],
        item["cause"],
        item["inspection"],
        item["action"],
        item["old_branch"],
        item["new_branch"],
        stamped,
    )
    if row:
        conn.execute(
            """
            UPDATE manuals
            SET keywords = ?, title = ?, cause = ?, inspection = ?, action = ?,
                old_branch = ?, new_branch = ?, updated_at = ?
            WHERE id = ?
            """,
            (*fields, int(row["id"])),
        )
    else:
        conn.execute(
            """
            INSERT INTO manuals (
                model_id, error_code, keywords, title, cause, inspection,
                action, old_branch, new_branch, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (int(model["id"]), item["error_code"], *fields),
        )
    logger.info("GPS장애 안내를 넣었습니다.")


def sync_run_field_symptoms(conn: sqlite3.Connection) -> None:
    """무정차·과속은 GPS 확인 순서, 개문주행은 DTG 열림·닫힘과 HIGH/LOW 확인이다."""
    stamped = now_text()
    model = conn.execute("SELECT id FROM models WHERE code = 'INT'").fetchone()
    if not model:
        return
    changed = False
    model_id = int(model["id"])
    for code in RUN_FIELD_CODES:
        item = next(
            (manual for manual in SEED_MANUALS if manual["model"] == "INT" and manual["error_code"] == code),
            None,
        )
        if not item:
            continue
        row = conn.execute(
            "SELECT id, inspection FROM manuals WHERE model_id = ? AND error_code = ?",
            (model_id, code),
        ).fetchone()
        if row and (row["inspection"] or "") == item["inspection"]:
            continue
        fields = (
            item["keywords"],
            item["title"],
            item["cause"],
            item["inspection"],
            item["action"],
            item["old_branch"],
            item["new_branch"],
            stamped,
        )
        if row:
            conn.execute(
                """
                UPDATE manuals
                SET keywords = ?, title = ?, cause = ?, inspection = ?, action = ?,
                    old_branch = ?, new_branch = ?, updated_at = ?
                WHERE id = ?
                """,
                (*fields, int(row["id"])),
            )
        else:
            conn.execute(
                """
                INSERT INTO manuals (
                    model_id, error_code, keywords, title, cause, inspection,
                    action, old_branch, new_branch, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (model_id, code, *fields),
            )
        changed = True
    if changed:
        logger.info("무정차, 개문주행, 과속 안내를 넣었습니다.")


def sync_ahead_blind(conn: sqlite3.Connection) -> None:
    """앞뒤차 안보임은 아이콘 상태에 따라 LTE, BMS, GPS로 가른다."""
    stamped = now_text()
    item = next((manual for manual in SEED_MANUALS if manual["error_code"] == AHEAD_CODE), None)
    if not item:
        return
    changed = False
    for code in ("INT", "LTE"):
        model = conn.execute("SELECT id FROM models WHERE code = ?", (code,)).fetchone()
        if not model:
            continue
        row = conn.execute(
            "SELECT id, inspection, keywords FROM manuals WHERE model_id = ? AND error_code = ?",
            (int(model["id"]), AHEAD_CODE),
        ).fetchone()
        if (
            row
            and (row["inspection"] or "") == item["inspection"]
            and (row["keywords"] or "") == item["keywords"]
        ):
            continue
        fields = (
            item["keywords"],
            item["title"],
            item["cause"],
            item["inspection"],
            item["action"],
            item["old_branch"],
            item["new_branch"],
            stamped,
        )
        if row:
            conn.execute(
                """
                UPDATE manuals
                SET keywords = ?, title = ?, cause = ?, inspection = ?, action = ?,
                    old_branch = ?, new_branch = ?, updated_at = ?
                WHERE id = ?
                """,
                (*fields, int(row["id"])),
            )
        else:
            conn.execute(
                """
                INSERT INTO manuals (
                    model_id, error_code, keywords, title, cause, inspection,
                    action, old_branch, new_branch, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (int(model["id"]), AHEAD_CODE, *fields),
            )
        changed = True
    if changed:
        logger.info("앞뒤차 안보임 안내를 넣었습니다.")


def sync_unsent_files(conn: sqlite3.Connection) -> None:
    """표출기 미전송파일은 LTE 통신불량이 풀리면 같이 전송된다."""
    stamped = now_text()
    item = next((manual for manual in SEED_MANUALS if manual["error_code"] == UNSENT_CODE), None)
    if not item:
        return
    changed = False
    for code in ("DISP", "LTE"):
        model = conn.execute("SELECT id FROM models WHERE code = ?", (code,)).fetchone()
        if not model:
            continue
        row = conn.execute(
            "SELECT id, inspection FROM manuals WHERE model_id = ? AND error_code = ?",
            (int(model["id"]), UNSENT_CODE),
        ).fetchone()
        if row and (row["inspection"] or "") == item["inspection"]:
            continue
        fields = (
            item["keywords"],
            item["title"],
            item["cause"],
            item["inspection"],
            item["action"],
            item["old_branch"],
            item["new_branch"],
            stamped,
        )
        if row:
            conn.execute(
                """
                UPDATE manuals
                SET keywords = ?, title = ?, cause = ?, inspection = ?, action = ?,
                    old_branch = ?, new_branch = ?, updated_at = ?
                WHERE id = ?
                """,
                (*fields, int(row["id"])),
            )
        else:
            conn.execute(
                """
                INSERT INTO manuals (
                    model_id, error_code, keywords, title, cause, inspection,
                    action, old_branch, new_branch, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (int(model["id"]), UNSENT_CODE, *fields),
            )
        changed = True
    if changed:
        logger.info("표출기 미전송파일 안내를 넣었습니다.")


def remove_receipt_manual(conn: sqlite3.Connection) -> None:
    """영수증 항목은 현장 안내에서 빼 둔다."""
    cur = conn.execute(
        """
        DELETE FROM manuals
        WHERE title = '영수증 출력 이상'
           OR error_code = 'E-204'
           OR keywords LIKE '%영수증%'
           OR cause LIKE '%영수증%'
           OR action LIKE '%영수증%'
        """
    )
    history = conn.execute(
        """
        DELETE FROM history
        WHERE query LIKE '%영수증%'
           OR matched_code = 'E-204'
           OR matched_title LIKE '%영수증%'
           OR cause LIKE '%영수증%'
           OR action LIKE '%영수증%'
        """
    )
    if cur.rowcount or history.rowcount:
        logger.info("영수증 매뉴얼을 제거했습니다.")


def sync_b800_gate_history(conn: sqlite3.Connection) -> None:
    """승하차 매뉴얼을 짧은 확인→조치 문장으로 맞춘다."""
    stamped = now_text()
    changed = False
    for code in ("GATE", "B650"):
        model = conn.execute("SELECT id FROM models WHERE code = ?", (code,)).fetchone()
        if not model:
            continue
        model_id = int(model["id"])
        for item in SEED_MANUALS:
            if item["model"] != code:
                continue
            if item["error_code"] != "통신불량" and "→" not in item["inspection"]:
                continue
            row = conn.execute(
                "SELECT id, inspection FROM manuals WHERE model_id = ? AND error_code = ?",
                (model_id, item["error_code"]),
            ).fetchone()
            current = row["inspection"] if row else ""
            if item["error_code"] == "통신불량" and "가포설" in current:
                continue
            if item["error_code"] != "통신불량" and "→" in current:
                continue
            if row:
                conn.execute(
                    """
                    UPDATE manuals
                    SET keywords = ?, title = ?, cause = ?, inspection = ?, action = ?,
                        old_branch = ?, new_branch = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        item["keywords"],
                        item["title"],
                        item["cause"],
                        item["inspection"],
                        item["action"],
                        item["old_branch"],
                        item["new_branch"],
                        stamped,
                        int(row["id"]),
                    ),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO manuals (
                        model_id, error_code, keywords, title, cause, inspection,
                        action, old_branch, new_branch, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        model_id,
                        item["error_code"],
                        item["keywords"],
                        item["title"],
                        item["cause"],
                        item["inspection"],
                        item["action"],
                        item["old_branch"],
                        item["new_branch"],
                        stamped,
                    ),
                )
            changed = True
    if changed:
        logger.info("승하차 매뉴얼을 짧은 확인 문장으로 바꿨습니다.")


def sync_gate_comm_vehicle(conn: sqlite3.Connection) -> None:
    """통신불량 OS·SAM 단계는 B800, B700만 두고 B710 안내에서는 뺀다."""
    stamped = now_text()
    changed = False
    for code in ("GATE", "B650"):
        model = conn.execute("SELECT id FROM models WHERE code = ?", (code,)).fetchone()
        item = next(
            (
                manual
                for manual in SEED_MANUALS
                if manual["model"] == code and manual["error_code"] == "통신불량"
            ),
            None,
        )
        if not model or not item:
            continue
        row = conn.execute(
            "SELECT id, inspection FROM manuals WHERE model_id = ? AND error_code = ?",
            (int(model["id"]), "통신불량"),
        ).fetchone()
        if row and (row["inspection"] or "") == item["inspection"]:
            continue
        fields = (
            item["keywords"],
            item["title"],
            item["cause"],
            item["inspection"],
            item["action"],
            item["old_branch"],
            item["new_branch"],
            stamped,
        )
        if row:
            conn.execute(
                """
                UPDATE manuals
                SET keywords = ?, title = ?, cause = ?, inspection = ?, action = ?,
                    old_branch = ?, new_branch = ?, updated_at = ?
                WHERE id = ?
                """,
                (*fields, int(row["id"])),
            )
        else:
            conn.execute(
                """
                INSERT INTO manuals (
                    model_id, error_code, keywords, title, cause, inspection,
                    action, old_branch, new_branch, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (int(model["id"]), "통신불량", *fields),
            )
        changed = True
    if changed:
        logger.info("통신불량 OS 단계를 기종별로 나눴습니다.")


def sync_gate_boot_fault(conn: sqlite3.Connection) -> None:
    """승하차 부팅불량은 통신·스위치 확인 후, 로고 멈춤과 검은 화면은 단말기 교체다."""
    stamped = now_text()
    changed = False
    for code in ("GATE", "B650"):
        model = conn.execute("SELECT id FROM models WHERE code = ?", (code,)).fetchone()
        item = next(
            (
                manual
                for manual in SEED_MANUALS
                if manual["model"] == code and manual["error_code"] == "부팅불량"
            ),
            None,
        )
        if not model or not item:
            continue
        row = conn.execute(
            "SELECT id, inspection FROM manuals WHERE model_id = ? AND error_code = ?",
            (int(model["id"]), "부팅불량"),
        ).fetchone()
        if row and (row["inspection"] or "") == item["inspection"]:
            continue
        fields = (
            item["keywords"],
            item["title"],
            item["cause"],
            item["inspection"],
            item["action"],
            item["old_branch"],
            item["new_branch"],
            stamped,
        )
        if row:
            conn.execute(
                """
                UPDATE manuals
                SET keywords = ?, title = ?, cause = ?, inspection = ?, action = ?,
                    old_branch = ?, new_branch = ?, updated_at = ?
                WHERE id = ?
                """,
                (*fields, int(row["id"])),
            )
        else:
            conn.execute(
                """
                INSERT INTO manuals (
                    model_id, error_code, keywords, title, cause, inspection,
                    action, old_branch, new_branch, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (int(model["id"]), "부팅불량", *fields),
            )
        changed = True
    if changed:
        logger.info("승하차 부팅불량 안내를 넣었습니다.")


def sync_gate_buzzer(conn: sqlite3.Connection) -> None:
    """승하차 부저음이 안 나오면 승하차 단말기를 교체한다."""
    stamped = now_text()
    changed = False
    for code in ("GATE", "B650"):
        model = conn.execute("SELECT id FROM models WHERE code = ?", (code,)).fetchone()
        item = next(
            (
                manual
                for manual in SEED_MANUALS
                if manual["model"] == code and manual["error_code"] == "부저음"
            ),
            None,
        )
        if not model or not item:
            continue
        row = conn.execute(
            "SELECT id, inspection FROM manuals WHERE model_id = ? AND error_code = ?",
            (int(model["id"]), "부저음"),
        ).fetchone()
        if row and (row["inspection"] or "") == item["inspection"]:
            continue
        fields = (
            item["keywords"],
            item["title"],
            item["cause"],
            item["inspection"],
            item["action"],
            item["old_branch"],
            item["new_branch"],
            stamped,
        )
        if row:
            conn.execute(
                """
                UPDATE manuals
                SET keywords = ?, title = ?, cause = ?, inspection = ?, action = ?,
                    old_branch = ?, new_branch = ?, updated_at = ?
                WHERE id = ?
                """,
                (*fields, int(row["id"])),
            )
        else:
            conn.execute(
                """
                INSERT INTO manuals (
                    model_id, error_code, keywords, title, cause, inspection,
                    action, old_branch, new_branch, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (int(model["id"]), "부저음", *fields),
            )
        changed = True
    if changed:
        logger.info("승하차 부저음 안내를 넣었습니다.")


def sync_gate_power_switch(conn: sqlite3.Connection) -> None:
    """승하차 전원불량은 전원 스위치 ON/OFF부터 확인한다."""
    stamped = now_text()
    changed = False
    for code in ("GATE", "B650"):
        model = conn.execute("SELECT id FROM models WHERE code = ?", (code,)).fetchone()
        if not model:
            continue
        item = next(
            (
                manual
                for manual in SEED_MANUALS
                if manual["model"] == code and manual["error_code"] == "전원불량"
            ),
            None,
        )
        if not item:
            continue
        row = conn.execute(
            "SELECT id, inspection FROM manuals WHERE model_id = ? AND error_code = ?",
            (int(model["id"]), "전원불량"),
        ).fetchone()
        if row and "스위치" in (row["inspection"] or ""):
            continue
        fields = (
            item["keywords"],
            item["title"],
            item["cause"],
            item["inspection"],
            item["action"],
            item["old_branch"],
            item["new_branch"],
            stamped,
        )
        if row:
            conn.execute(
                """
                UPDATE manuals
                SET keywords = ?, title = ?, cause = ?, inspection = ?, action = ?,
                    old_branch = ?, new_branch = ?, updated_at = ?
                WHERE id = ?
                """,
                (*fields, int(row["id"])),
            )
        else:
            conn.execute(
                """
                INSERT INTO manuals (
                    model_id, error_code, keywords, title, cause, inspection,
                    action, old_branch, new_branch, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (int(model["id"]), "전원불량", *fields),
            )
        changed = True
    if changed:
        logger.info("승하차 전원불량에 스위치 확인을 넣었습니다.")


def replace_sample_catalog(conn: sqlite3.Connection) -> None:
    """이전 샘플 기종(모델 A/B/C)만 있을 때 실제 차량 구성으로 바꾼다."""
    codes = {row["code"] for row in conn.execute("SELECT code FROM models")}
    if not codes or not codes <= {"A", "B", "C"} or "INT" in codes:
        return
    conn.execute("DELETE FROM models")
    seed(conn)
    logger.info("기종을 B800, B710, B700 구성으로 바꿨습니다.")


def device_model_code(system_code: str, device_code: str) -> str:
    if device_code == "GATE" and system_code == "B710":
        return "B650"
    return device_code


GATE_POSITIONS = {
    "BOARD": {"name": "승차", "number": "1"},
    "ALIGHT1": {"name": "하차1", "number": "2"},
    "ALIGHT2": {"name": "하차2", "number": "3"},
}


def device_hint(system_code: str, device_code: str) -> str:
    gate_count = "버스에 따라 1대에서 3대까지 달립니다. 승차=1번, 하차1=2번, 하차2=3번이며 기종은 같습니다."
    if device_code == "GATE" and system_code == "B710":
        return f"이 차량의 승하차단말기는 B650입니다. {gate_count}"
    if device_code == "GATE":
        return f"B800과 B700은 같은 승하차단말기를 사용합니다. {gate_count}"
    return "B800, B710, B700 공통 단말기입니다."


def detect_position(query: str) -> str:
    """검색어 안의 승차, 하차1, 하차2를 위치 코드로 읽는다."""
    compact = re.sub(r"\s+", "", query or "")
    aliases = (
        ("ALIGHT2", ("하차2", "승하차단말기3", "단말기3")),
        ("ALIGHT1", ("하차1", "승하차단말기2", "단말기2")),
        ("BOARD", ("승차", "승하차단말기1", "단말기1")),
    )
    for code, names in aliases:
        if any(name in compact for name in names):
            return code
    return ""


def apply_position_name(text: str, position_code: str) -> str:
    item = GATE_POSITIONS.get(position_code)
    if item:
        return (text or "").replace("{위치}", item["name"])
    value = text or ""
    value = value.replace("통합단말기 {위치}케이블", "통합단말기 케이블")
    value = value.replace("통합단말기 {위치} ", "통합단말기 ")
    value = value.replace("{위치} ", "")
    return value.replace("{위치}", "")


def model_codes_for_scope(
    system_code: str,
    device_code: str,
    extra_codes: Optional[list[str]] = None,
) -> list[str]:
    """전체가 기본이다. 기종이나 단말기를 골랐을 때만 그 범위로 좁힌다.

    단말기를 고르지 않은(전체) 검색에는 사용자가 추가한 단말기도 포함한다.
    """
    if device_code == "GATE":
        if system_code == "B710":
            return ["B650"]
        if system_code in {"B800", "B700"}:
            return ["GATE"]
        return ["GATE", "B650"]
    if device_code in {"INT", "DISP", "LTE"}:
        return [device_code]
    shared = ["INT", "DISP", "LTE"]
    if system_code == "B710":
        codes = shared + ["B650"]
    elif system_code in {"B800", "B700"}:
        codes = shared + ["GATE"]
    else:
        codes = shared + ["GATE", "B650"]
    if not device_code and extra_codes:
        for code in extra_codes:
            if code and code not in codes and code not in BUILTIN_MODEL_CODES:
                codes.append(code)
    return codes


def list_custom_model_codes(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        "SELECT code FROM models WHERE code NOT IN (?, ?, ?, ?, ?) ORDER BY id",
        tuple(sorted(BUILTIN_MODEL_CODES)),
    ).fetchall()
    return [str(row["code"]) for row in rows]


def position_label(position_code: str) -> str:
    item = GATE_POSITIONS.get(position_code)
    if not item:
        return ""
    return f"{item['name']} · 승하차단말기 {item['number']}"


def position_line(position_code: str) -> str:
    item = GATE_POSITIONS[position_code]
    return (
        f"위치: {item['name']} · {item['number']}번"
    )


def _is_ready_branch(raw: str) -> bool:
    return raw.startswith(("구형", "신형", "B800", "B710", "B700", "B650", "공통", "이 절차", "이 차량"))


def compose_inspection(inspection: str, old_branch: str, new_branch: str) -> str:
    """분기 문장을 붙인다. 기종 안내는 그대로 두고, 버전 메모만 구형/신형으로 표시한다."""
    lines = [inspection.strip()] if inspection and inspection.strip() else []

    def append_branch(raw: str, version_label: str) -> None:
        raw = (raw or "").strip()
        if not raw or raw in "\n".join(lines):
            return
        lines.append(raw if _is_ready_branch(raw) else f"{version_label}: {raw}")

    append_branch(old_branch, "구형 버전일 경우")
    append_branch(new_branch, "신형 버전일 경우")
    text = "\n".join(lines)
    vehicle_note = any(token in text for token in ("B800", "B710", "B700", "B650", "공통"))
    if not vehicle_note and "구형" not in text:
        lines.append("구형 버전일 경우: 추가 분해 없이 전원, 커넥터, 코드만 재확인합니다.")
    if not vehicle_note and "신형" not in text:
        lines.append("신형 버전일 경우: 진단 화면에서 같은 항목을 재확인하고 코드를 기록합니다.")
    return "\n".join(lines)


def score_manual(manual: dict, query: str) -> int:
    compact_query = normalize(query)
    if not compact_query:
        return 0
    score = 0
    code = normalize(manual["error_code"])
    title = normalize(manual["title"])
    keywords = [normalize(part) for part in manual["keywords"].split(",") if normalize(part)]

    if compact_query == code:
        score += 120
    elif code and (code in compact_query or compact_query in code):
        score += 80
    if compact_query and compact_query in title:
        score += 40
    for keyword in keywords:
        if len(keyword) < 2:
            continue
        if keyword in compact_query or compact_query in keyword:
            score += 30

    for token in re.split(r"[\s,/+]+", query.lower()):
        token_n = normalize(token)
        if len(token_n) < 2:
            continue
        if code and (token_n == code or token_n in code):
            score += 40
        if token_n in title:
            score += 15
        if any(token_n in keyword or keyword in token_n for keyword in keywords if len(keyword) >= 2):
            score += 20
    return score


def rank_manuals(manuals: list[dict], query: str) -> list[tuple[int, dict]]:
    ranked = [(score_manual(manual, query), manual) for manual in manuals]
    ranked = [item for item in ranked if item[0] >= MIN_SCORE]
    ranked.sort(key=lambda item: (-item[0], item[1]["error_code"]))
    return ranked


def fallback_guide() -> dict[str, str]:
    return {
        "cause": "등록된 현장 증상이 없습니다.",
        "inspection": compose_inspection(
            "기종·단말기·위치 확인\n승하차: 통신불량 · 카드무감 · 전원불량 · 화면불량 · 부팅불량 · 부저음 · SAM통신오류 · PSAM 초기화 실패(PSAM 오류·SAM 오류)\n통합·표출: 단말기 전체 전원 불량\n표출: 화면불량 · 부팅불량 · 리커버리모드 · 음성불량 · 미전송파일\n통합: 통합연결대기중 · BMS 불량 · 펜소음 · 다인승 멈춤 · 단인승 준비 실패 · GPS장애 · 앞뒤차 안보임 · 무정차 · 개문주행 · 과속\nB700 통합: 신호등 및 속도 미표출\nLTE: LTE모뎀 불량\nLTE(B700·B710): 적색불 · ST1 미점등 · ST2 미점등\n케이블이 빠졌는지만 확인",
            "공통: 통합·표출·LTE",
            "B710 승하차만 B650",
        ),
        "action": "증상을 다시 입력하거나 데이터 관리에서 등록하세요.",
    }


def keeps_written_steps(manual: dict) -> bool:
    """번호로 적은 처리 순서는 그대로 보여 준다."""
    inspection = manual.get("inspection") or ""
    if "{위치}" in inspection:
        return True
    return not (manual.get("old_branch") or "").strip() and not (manual.get("new_branch") or "").strip()


def manual_guide(manual: dict) -> dict[str, str]:
    inspection = manual["inspection"].strip()
    if not keeps_written_steps(manual):
        inspection = compose_inspection(inspection, manual["old_branch"], manual["new_branch"])
    return {
        "cause": manual["cause"].strip(),
        "inspection": inspection,
        "action": manual["action"].strip(),
    }


def gemini_enabled() -> bool:
    return bool(os.getenv("GEMINI_API_KEY", "").strip())


def build_prompt(model: dict, query: str, manuals: list[dict]) -> str:
    blocks = []
    for manual in manuals:
        blocks.append(
            "\n".join(
                [
                    f"에러코드: {manual['error_code']}",
                    f"제목: {manual['title']}",
                    f"키워드: {manual['keywords']}",
                    f"원인: {manual['cause']}",
                    f"점검: {manual['inspection']}",
                    f"구형: {manual['old_branch'] or '별도 분기 없음'}",
                    f"신형: {manual['new_branch'] or '별도 분기 없음'}",
                    f"조치: {manual['action']}",
                ]
            )
        )
    manual_text = "\n---\n".join(blocks) if blocks else "없음"
    return f"""당신은 버스 운임단말기 현장 유지보수 안내원입니다.
현장 기술사가 바로 읽을 짧은 한국어로만 답합니다.
근거는 아래 매뉴얼뿐입니다. 매뉴얼에 없는 부품번호, 분해 절차, 전압 개조, 설정값 변경을 만들어내지 마세요.
점검에는 반드시 구형 버전과 신형 버전 분기를 넣으세요. 매뉴얼 문장을 유지하고, 없는 분기는 "매뉴얼과 동일"이라고 적으세요.
JSON 한 개만 출력합니다. 키는 cause, inspection, action 입니다.
cause는 핵심 원인 1~2문장, inspection은 번호가 있는 점검 순서, action은 지금 할 조치 1~2문장입니다.

기종명: {model['name']}
기종 메모: {model['note']}
현장 입력: {query}
매뉴얼:
{manual_text}
"""


def extract_json(text: str) -> dict[str, str]:
    raw = text.strip()
    if raw.startswith("```"):
        raw = re.sub(r"^```(?:json)?", "", raw).strip()
        raw = re.sub(r"```$", "", raw).strip()
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", raw, re.DOTALL)
        if not match:
            raise ValueError("Gemini 응답에서 JSON을 찾지 못했습니다.")
        data = json.loads(match.group(0))
    if not isinstance(data, dict):
        raise ValueError("Gemini 응답 형식이 올바르지 않습니다.")
    result = {}
    for key in ("cause", "inspection", "action"):
        value = str(data.get(key, "")).strip()
        if not value:
            raise ValueError("Gemini 응답에 비어 있는 항목이 있습니다.")
        result[key] = value
    return result


async def gemini_once(prompt: str, json_mode: bool) -> dict[str, str]:
    api_key = os.getenv("GEMINI_API_KEY", "").strip()
    model_name = os.getenv("GEMINI_MODEL", "gemini-2.5-flash").strip() or "gemini-2.5-flash"
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent"
    payload = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.2, "maxOutputTokens": 1024},
    }
    if json_mode:
        payload["generationConfig"]["responseMimeType"] = "application/json"
    async with httpx.AsyncClient(timeout=20) as client:
        response = await client.post(url, params={"key": api_key}, json=payload)
        response.raise_for_status()
        body = response.json()
    candidates = body.get("candidates") or []
    if not candidates:
        raise ValueError("Gemini 응답이 비어 있습니다.")
    parts = candidates[0].get("content", {}).get("parts") or []
    text = "\n".join(part.get("text", "") for part in parts if part.get("text"))
    return extract_json(text)


async def call_gemini(prompt: str) -> dict[str, str]:
    try:
        return await gemini_once(prompt, True)
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code != 400:
            raise
        logger.warning("Gemini JSON 모드가 거절되어 일반 모드로 다시 요청합니다. status=%s", exc.response.status_code)
        return await gemini_once(prompt, False)
    except ValueError:
        logger.warning("Gemini JSON 해석에 실패해 일반 모드로 다시 요청합니다.")
        return await gemini_once(prompt, False)


def related_payload(ranked: list[tuple[int, dict]]) -> list[dict]:
    items = []
    for score, manual in ranked[:3]:
        items.append(
            {
                "id": manual["id"],
                "error_code": manual["error_code"],
                "title": manual["title"],
                "score": score,
            }
        )
    return items


def save_history(conn: sqlite3.Connection, record: dict) -> tuple[int, str]:
    created = now_text()
    cur = conn.execute(
        """
        INSERT INTO history (
            model_id, model_name, query, cause, inspection, action, source,
            matched_code, matched_title, manual_id, notice, created_at, system_code, position_code
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            record["model_id"],
            record["model_name"],
            record["query"],
            record["cause"],
            record["inspection"],
            record["action"],
            record["source"],
            record["matched_code"],
            record["matched_title"],
            record["manual_id"],
            record["notice"],
            created,
            record.get("system_code", ""),
            record.get("position_code", ""),
        ),
    )
    return int(cur.lastrowid), created


class ModelIn(BaseModel):
    name: str
    code: str = ""
    note: str = ""

    @model_validator(mode="after")
    def clean(self) -> "ModelIn":
        self.name = self.name.strip()
        self.code = self.code.strip().upper()
        self.note = self.note.strip()
        if not self.name:
            raise ValueError("기종 이름을 입력하세요.")
        if len(self.name) > 80 or len(self.note) > 400 or len(self.code) > 20:
            raise ValueError("기종 정보가 너무 깁니다.")
        return self


class ManualIn(BaseModel):
    model_id: int
    error_code: str
    keywords: str = ""
    title: str
    cause: str
    inspection: str
    action: str
    old_branch: str = ""
    new_branch: str = ""

    @model_validator(mode="after")
    def clean(self) -> "ManualIn":
        self.error_code = self.error_code.strip().upper()
        self.keywords = ",".join(
            part.strip() for part in self.keywords.replace("，", ",").split(",") if part.strip()
        )
        self.title = self.title.strip()
        self.cause = self.cause.strip()
        self.inspection = self.inspection.strip()
        self.action = self.action.strip()
        self.old_branch = self.old_branch.strip()
        self.new_branch = self.new_branch.strip()
        if not self.error_code or not self.title or not self.cause or not self.inspection or not self.action:
            raise ValueError("증상, 제목, 원인, 점검, 조치는 비울 수 없습니다.")
        limits = (
            ("증상", self.error_code, 40),
            ("제목", self.title, 120),
            ("키워드", self.keywords, 400),
            ("원인", self.cause, 2000),
            ("점검", self.inspection, 4000),
            ("조치", self.action, 2000),
            ("구형 분기", self.old_branch, 2000),
            ("신형 분기", self.new_branch, 2000),
        )
        for label, value, limit in limits:
            if len(value) > limit:
                raise ValueError(f"{label}은 {limit}자 이하로 입력하세요.")
        return self


class DiagnoseIn(BaseModel):
    model_id: int = 0
    query: str
    system_code: str = ""
    device_code: str = ""
    position_code: str = ""

    @model_validator(mode="after")
    def clean(self) -> "DiagnoseIn":
        self.query = " ".join(self.query.split())
        self.system_code = self.system_code.strip().upper()
        self.device_code = self.device_code.strip().upper()
        self.position_code = self.position_code.strip().upper()
        if not self.query:
            raise ValueError("현장 증상을 입력하세요.")
        if len(self.query) > 200:
            raise ValueError("입력은 200자 이하로 줄여 주세요.")
        if self.system_code and self.system_code not in SYSTEMS:
            raise ValueError("기종은 B800, B710, B700 중에서 선택하세요.")
        known_devices = {item["code"] for item in DEVICE_DEFS}
        if self.device_code and self.device_code not in known_devices and parse_custom_device_code(self.device_code) is None:
            raise ValueError("단말기를 다시 선택해 주세요.")
        if self.position_code and self.position_code not in GATE_POSITIONS:
            raise ValueError("승하차 위치는 승차, 하차1, 하차2 중에서 선택하세요.")
        return self


def log_startup() -> None:
    init_db()
    port = os.getenv("PORT", "8000")
    ips = []
    try:
        for info in socket.getaddrinfo(socket.gethostname(), None):
            ip = info[4][0]
            if "." in ip and not ip.startswith("127."):
                ips.append(ip)
    except OSError:
        ips = []
    logger.info("현장 가이드 준비 완료. PC: http://127.0.0.1:%s", port)
    if ips:
        logger.info("같은 네트워크 휴대폰: %s", ", ".join(f"http://{ip}:{port}" for ip in sorted(set(ips))))
    logger.info("Gemini 모드: %s", "사용" if gemini_enabled() else "내부 매뉴얼")


@asynccontextmanager
async def lifespan(_: FastAPI):
    log_startup()
    yield


app = FastAPI(title="버스 단말기 현장 가이드", version="1.0.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.exception_handler(RequestValidationError)
async def validation_handler(_: object, exc: RequestValidationError) -> JSONResponse:
    messages = []
    for err in exc.errors():
        msg = str(err.get("msg", "")).replace("Value error, ", "")
        if msg:
            messages.append(msg)
    return JSONResponse(status_code=422, content={"detail": " ".join(messages) or "입력값을 확인하세요."})


@app.get("/")
def home() -> FileResponse:
    if not INDEX_PATH.exists():
        raise HTTPException(status_code=404, detail="index.html 이 없습니다.")
    return FileResponse(INDEX_PATH, media_type="text/html; charset=utf-8", headers={"Cache-Control": "no-cache"})


@app.get("/favicon.ico")
def favicon() -> Response:
    return Response(status_code=204)


@app.get("/api/status")
def status() -> dict:
    with db() as conn:
        manuals = conn.execute("SELECT COUNT(*) AS c FROM manuals").fetchone()["c"]
        models = conn.execute("SELECT COUNT(*) AS c FROM models").fetchone()["c"]
    return {
        "gemini_enabled": gemini_enabled(),
        "gemini_model": os.getenv("GEMINI_MODEL", "gemini-2.5-flash"),
        "manual_count": manuals,
        "model_count": models,
        "db_path": str(DB_PATH),
    }


def model_by_code(conn: sqlite3.Connection, code: str) -> Optional[dict]:
    row = conn.execute("SELECT * FROM models WHERE code = ?", (code,)).fetchone()
    return dict(row) if row else None


def resolve_model(conn: sqlite3.Connection, model_id: int, system_code: str) -> dict:
    row = conn.execute("SELECT * FROM models WHERE id = ?", (model_id,)).fetchone()
    if not row:
        raise HTTPException(status_code=400, detail="선택한 단말기가 없습니다.")
    model = dict(row)
    target_code = ""
    if system_code == "B710" and model["code"] == "GATE":
        target_code = "B650"
    elif system_code in {"B800", "B700"} and model["code"] == "B650":
        target_code = "GATE"
    if not target_code:
        return model
    swapped = model_by_code(conn, target_code)
    return swapped or model


def history_label(system_code: str, model: dict, position_code: str = "") -> str:
    if model["code"] == "B650":
        name = "승하차단말기 B650"
    else:
        name = model["name"]
    label = f"{system_code} · {name}" if system_code else f"전체 · {name}"
    position = position_label(position_code)
    return f"{label} · {position}" if position else label


def custom_device_code(model_id: int) -> str:
    return f"M{int(model_id)}"


def parse_custom_device_code(device_code: str) -> Optional[int]:
    if re.fullmatch(r"M\d+", device_code or ""):
        return int(device_code[1:])
    return None


@app.get("/api/systems")
def list_systems() -> list[dict]:
    with db() as conn:
        all_models = [dict(row) for row in conn.execute("SELECT * FROM models ORDER BY id")]
    models = {row["code"]: row for row in all_models}
    extras = [row for row in all_models if row["code"] not in BUILTIN_MODEL_CODES]
    payload = []
    for code, note in SYSTEMS.items():
        devices = []
        for device in DEVICE_DEFS:
            model = models.get(device_model_code(code, device["code"]))
            if not model:
                continue
            devices.append(
                {
                    "code": device["code"],
                    "label": device["label"],
                    "model_id": model["id"],
                    "model_name": model["name"],
                    "model_code": model["code"],
                    "hint": device_hint(code, device["code"]),
                    "custom": False,
                }
            )
        for model in extras:
            devices.append(
                {
                    "code": custom_device_code(int(model["id"])),
                    "label": model["name"],
                    "model_id": model["id"],
                    "model_name": model["name"],
                    "model_code": model["code"],
                    "hint": model["note"] or "추가로 등록한 단말기입니다.",
                    "custom": True,
                }
            )
        payload.append({"code": code, "name": code, "note": note, "devices": devices})
    return payload


@app.get("/api/models")
def list_models() -> list[dict]:
    with db() as conn:
        rows = [dict(row) for row in conn.execute("SELECT * FROM models").fetchall()]
        order = {"INT": 1, "DISP": 2, "LTE": 3, "GATE": 4, "B650": 5}
        rows.sort(key=lambda row: (order.get(row["code"], 50), row["name"]))
        return rows


@app.post("/api/models", status_code=201)
def create_model(body: ModelIn) -> dict:
    code = body.code or re.sub(r"\s+", "", body.name)[:12].upper()
    with db() as conn:
        try:
            cur = conn.execute(
                "INSERT INTO models (name, code, note) VALUES (?, ?, ?)",
                (body.name, code, body.note),
            )
            new_id = int(cur.lastrowid)
        except sqlite3.IntegrityError:
            raise HTTPException(status_code=409, detail="같은 이름의 단말기가 있습니다.") from None
        row = conn.execute("SELECT * FROM models WHERE id = ?", (new_id,)).fetchone()
        return dict(row)


@app.delete("/api/models/{model_id}", status_code=204)
def delete_model(model_id: int) -> Response:
    with db() as conn:
        cur = conn.execute("DELETE FROM models WHERE id = ?", (model_id,))
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="단말기를 찾을 수 없습니다.")
    return Response(status_code=204)


def fetch_manuals(conn: sqlite3.Connection, model_id: Optional[int] = None, q: str = "") -> list[dict]:
    sql = """
        SELECT m.*, mo.name AS model_name, mo.code AS model_code
        FROM manuals m
        JOIN models mo ON mo.id = m.model_id
        WHERE 1 = 1
    """
    params: list[object] = []
    if model_id:
        sql += " AND m.model_id = ?"
        params.append(model_id)
    if q:
        pat = like_pattern(q)
        sql += """
            AND (
                m.error_code LIKE ? ESCAPE '\\'
                OR m.title LIKE ? ESCAPE '\\'
                OR m.keywords LIKE ? ESCAPE '\\'
                OR m.cause LIKE ? ESCAPE '\\'
                OR m.action LIKE ? ESCAPE '\\'
            )
        """
        params.extend([pat, pat, pat, pat, pat])
    sql += " ORDER BY mo.name, m.error_code"
    return [dict(row) for row in conn.execute(sql, params).fetchall()]


@app.get("/api/manuals")
def list_manuals(
    model_id: Optional[int] = None,
    q: str = Query(default="", max_length=200),
) -> list[dict]:
    with db() as conn:
        return fetch_manuals(conn, model_id, q.strip())


def require_model(conn: sqlite3.Connection, model_id: int) -> dict:
    row = conn.execute("SELECT * FROM models WHERE id = ?", (model_id,)).fetchone()
    if not row:
        raise HTTPException(status_code=400, detail="선택한 기종이 없습니다.")
    return dict(row)


@app.post("/api/manuals", status_code=201)
def create_manual(body: ManualIn) -> dict:
    with db() as conn:
        require_model(conn, body.model_id)
        try:
            cur = conn.execute(
                """
                INSERT INTO manuals (
                    model_id, error_code, keywords, title, cause, inspection,
                    action, old_branch, new_branch, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    body.model_id,
                    body.error_code,
                    body.keywords,
                    body.title,
                    body.cause,
                    body.inspection,
                    body.action,
                    body.old_branch,
                    body.new_branch,
                    now_text(),
                ),
            )
            new_id = int(cur.lastrowid)
        except sqlite3.IntegrityError:
            raise HTTPException(status_code=409, detail="같은 단말기에 이미 등록된 증상입니다.") from None
        rows = fetch_manuals(conn)
        return next(row for row in rows if row["id"] == new_id)


@app.put("/api/manuals/{manual_id}")
def update_manual(manual_id: int, body: ManualIn) -> dict:
    with db() as conn:
        require_model(conn, body.model_id)
        try:
            cur = conn.execute(
                """
                UPDATE manuals
                SET model_id = ?, error_code = ?, keywords = ?, title = ?, cause = ?,
                    inspection = ?, action = ?, old_branch = ?, new_branch = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    body.model_id,
                    body.error_code,
                    body.keywords,
                    body.title,
                    body.cause,
                    body.inspection,
                    body.action,
                    body.old_branch,
                    body.new_branch,
                    now_text(),
                    manual_id,
                ),
            )
        except sqlite3.IntegrityError:
            raise HTTPException(status_code=409, detail="같은 단말기에 이미 등록된 증상입니다.") from None
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="매뉴얼을 찾을 수 없습니다.")
        rows = fetch_manuals(conn)
        return next(row for row in rows if row["id"] == manual_id)


@app.delete("/api/manuals/{manual_id}", status_code=204)
def delete_manual(manual_id: int) -> Response:
    with db() as conn:
        cur = conn.execute("DELETE FROM manuals WHERE id = ?", (manual_id,))
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="매뉴얼을 찾을 수 없습니다.")
    return Response(status_code=204)


@app.post("/api/diagnose")
async def diagnose(body: DiagnoseIn) -> dict:
    notices: list[str] = []
    detected = detect_position(body.query)
    if detected:
        body.position_code = detected
    custom_id = parse_custom_device_code(body.device_code)
    if custom_id is not None:
        body.model_id = custom_id
        body.device_code = ""
        body.position_code = ""
    with db() as conn:
        if body.device_code or not body.model_id:
            manuals = []
            model_dict = None
            extras = list_custom_model_codes(conn) if not body.device_code else []
            for code in model_codes_for_scope(body.system_code, body.device_code, extras):
                found = model_by_code(conn, code)
                if not found:
                    continue
                if model_dict is None:
                    model_dict = found
                manuals.extend(fetch_manuals(conn, int(found["id"])))
            if model_dict is None:
                raise HTTPException(status_code=400, detail="찾을 단말기가 없습니다.")
        else:
            model_dict = resolve_model(conn, body.model_id, body.system_code)
            if model_dict["code"] not in {"GATE", "B650"}:
                body.position_code = ""
            manuals = fetch_manuals(conn, model_dict["id"])
    if body.system_code == "B800":
        manuals = [item for item in manuals if item.get("error_code") not in LTE_B700_B710_CODES]
        compact = normalize(body.query)
        limited = ("적색불", "ST1", "ST2")
        if any(normalize(token) in compact for token in limited):
            st_specific = "st1" in compact or "st2" in compact
            manuals = [
                item
                for item in manuals
                if (not st_specific and item.get("error_code") == LTE_FAULT_CODE)
                or any(
                    normalize(token) in normalize(f"{item.get('error_code', '')} {item.get('title', '')}")
                    for token in limited
                )
            ]
    if body.system_code in {"B800", "B710"}:
        manuals = [item for item in manuals if item.get("error_code") not in B700_INT_CODES]
        compact = normalize(body.query)
        if "신호등" in compact:
            manuals = [
                item
                for item in manuals
                if "신호등" in normalize(f"{item.get('error_code', '')} {item.get('title', '')}")
            ]
    compact = normalize(body.query)
    whole_power = any(token in compact for token in ("전체전원", "단말기전원", "표출전원", "통합전원"))
    gate_power = any(token in compact for token in ("승하차", "승차", "하차"))
    if whole_power and not gate_power:
        manuals = [
            item
            for item in manuals
            if not (item.get("error_code") == "전원불량" and item.get("model_code") in {"GATE", "B650"})
        ]
    display_fault = "표출" in compact and any(token in compact for token in ("화면불량", "부팅", "리커버리"))
    if display_fault and not gate_power:
        manuals = [
            item
            for item in manuals
            if not (item.get("error_code") == "화면불량" and item.get("model_code") in {"GATE", "B650"})
        ]
    if "sam오류" in compact:
        manuals = [item for item in manuals if item.get("error_code") == "PSAM 초기화 실패"]
    if "미전송" in compact:
        manuals = [item for item in manuals if item.get("error_code") == UNSENT_CODE]
    ahead_hidden = any(token in compact for token in ("앞뒤차", "앞차", "뒤차"))
    st_specific = "st1" in compact or "st2" in compact
    lte_fault = (not st_specific) and (
        "lte모뎀불량" in compact
        or "적색불" in compact
        or "미점등" in compact
        or "g아이콘" in compact
        or "g만빼고" in compact
        or "g빼고" in compact
    )
    gate_screen = any(token in compact for token in ("승하차", "승차", "하차"))
    screen_white_black = (
        any(token in compact for token in ("백화", "흑화", "하얗", "하얀", "흰화면", "검은화면", "검은색"))
        and not gate_screen
        and not any(token in compact for token in ("부팅", "led", "상태표시", "리커버리"))
    )
    screen_hidden = (
        (
            "화면" in compact
            and "안보임" in compact
            and not gate_screen
        )
        or screen_white_black
    ) and not ahead_hidden
    if ahead_hidden:
        manuals = [item for item in manuals if item.get("error_code") == AHEAD_CODE]
        if not manuals:
            with db() as conn:
                for code in ("INT", "LTE"):
                    found = model_by_code(conn, code)
                    if not found:
                        continue
                    manuals.extend(
                        item
                        for item in fetch_manuals(conn, int(found["id"]))
                        if item.get("error_code") == AHEAD_CODE
                    )
    elif screen_hidden:
        manuals = [item for item in manuals if item.get("error_code") == "표출단말기 화면불량"]
        if not manuals:
            with db() as conn:
                found = model_by_code(conn, "DISP")
                if found:
                    manuals = [
                        item
                        for item in fetch_manuals(conn, int(found["id"]))
                        if item.get("error_code") == "표출단말기 화면불량"
                    ]
                    model_dict = found
    gate_boot = (not ahead_hidden) and (
        ("부팅" in compact and gate_screen)
        or "t머니" in compact
        or "티머니" in compact
        or (
            "부팅" in compact
            and "검은" in compact
            and "표출" not in compact
            and "led" not in compact
            and "상태표시" not in compact
        )
    )
    if gate_boot:
        manuals = [
            item
            for item in manuals
            if item.get("error_code") == "부팅불량" and item.get("model_code") in {"GATE", "B650"}
        ]
        if not manuals:
            if body.system_code == "B710":
                boot_codes = ["B650"]
            elif body.system_code in {"B800", "B700"}:
                boot_codes = ["GATE"]
            else:
                boot_codes = ["GATE", "B650"]
            with db() as conn:
                for code in boot_codes:
                    found = model_by_code(conn, code)
                    if not found:
                        continue
                    manuals.extend(
                        item
                        for item in fetch_manuals(conn, int(found["id"]))
                        if item.get("error_code") == "부팅불량"
                    )
                    if model_dict is None or body.system_code:
                        model_dict = found
    gate_buzzer = "부저" in compact and not ahead_hidden
    if gate_buzzer:
        manuals = [
            item
            for item in manuals
            if item.get("error_code") == "부저음" and item.get("model_code") in {"GATE", "B650"}
        ]
        if not manuals:
            if body.system_code == "B710":
                buzzer_codes = ["B650"]
            elif body.system_code in {"B800", "B700"}:
                buzzer_codes = ["GATE"]
            else:
                buzzer_codes = ["GATE", "B650"]
            with db() as conn:
                for code in buzzer_codes:
                    found = model_by_code(conn, code)
                    if not found:
                        continue
                    manuals.extend(
                        item
                        for item in fetch_manuals(conn, int(found["id"]))
                        if item.get("error_code") == "부저음"
                    )
    if "음성" in compact and not ahead_hidden:
        manuals = [item for item in manuals if item.get("error_code") == "표출단말기 음성불량"]
        if not manuals:
            with db() as conn:
                found = model_by_code(conn, "DISP")
                if found:
                    manuals = [
                        item
                        for item in fetch_manuals(conn, int(found["id"]))
                        if item.get("error_code") == "표출단말기 음성불량"
                    ]
                    model_dict = found
    if lte_fault and not ahead_hidden:
        manuals = [item for item in manuals if item.get("error_code") == LTE_FAULT_CODE]
        if not manuals:
            with db() as conn:
                found = model_by_code(conn, "LTE")
                if found:
                    manuals = [
                        item
                        for item in fetch_manuals(conn, int(found["id"]))
                        if item.get("error_code") == LTE_FAULT_CODE
                    ]
                    model_dict = found
    english_screen = "영어" in compact and not gate_power
    if english_screen:
        manuals = [item for item in manuals if item.get("error_code") == "표출단말기 리커버리모드"]
        if not manuals:
            with db() as conn:
                found = model_by_code(conn, "DISP")
                if found:
                    manuals = [
                        item
                        for item in fetch_manuals(conn, int(found["id"]))
                        if item.get("error_code") == "표출단말기 리커버리모드"
                    ]
                    model_dict = found
    run_code = ""
    if "무정차" in compact:
        run_code = "무정차"
    elif "과속" in compact:
        run_code = "과속"
    elif "개문" in compact:
        run_code = "개문주행"
    if run_code:
        manuals = [item for item in manuals if item.get("error_code") == run_code]
        if not manuals:
            with db() as conn:
                found = model_by_code(conn, "INT")
                if found:
                    manuals = [
                        item
                        for item in fetch_manuals(conn, int(found["id"]))
                        if item.get("error_code") == run_code
                    ]
                    model_dict = found
    ranked = rank_manuals(manuals, body.query)
    once_codes = {POWER_WHOLE_CODE, AHEAD_CODE, UNSENT_CODE}
    seen_codes: set[str] = set()
    collapsed = []
    for item in ranked:
        code = item[1].get("error_code")
        if code in once_codes:
            if code in seen_codes:
                continue
            seen_codes.add(code)
        collapsed.append(item)
    ranked = collapsed

    def scope_key(item: tuple[int, dict]) -> tuple:
        code = item[1].get("model_code", "")
        if body.system_code == "B710":
            bias = 0 if code == "B650" else 1
        else:
            bias = 1 if code == "B650" else 0
        return (-item[0], bias, item[1]["error_code"])

    ranked.sort(key=scope_key)
    top = ranked[0][1] if ranked else None
    if top:
        matched_model = next((item for item in manuals if item["id"] == top["id"]), None)
        if matched_model:
            model_dict = {
                "id": matched_model["model_id"],
                "name": matched_model["model_name"],
                "code": matched_model["model_code"],
                "note": model_dict.get("note", ""),
            }
    if top and keeps_written_steps(top):
        ranked = [item for item in ranked if item[1]["id"] == top["id"]] or ranked
    guide: Optional[dict[str, str]] = None
    source = "fallback"
    prompt_model = dict(model_dict)
    if body.system_code:
        prompt_model["note"] = f"선택 차량: {body.system_code}. {model_dict['note']}"
    if body.position_code:
        prompt_model["note"] += f" 점검 위치: {position_label(body.position_code)}."
    if top and keeps_written_steps(top):
        guide = manual_guide(top)
        source = "manual"
    elif top and gemini_enabled():
        try:
            guide = await call_gemini(build_prompt(prompt_model, body.query, [item[1] for item in ranked[:3]]))
            guide["inspection"] = compose_inspection(guide["inspection"], top["old_branch"], top["new_branch"])
            source = "gemini"
        except Exception as exc:
            logger.warning("Gemini 호출 실패, 내부 매뉴얼로 대체합니다: %s", exc.__class__.__name__)
            guide = manual_guide(top)
            source = "manual"
            notices.append("Gemini 호출에 실패해 내부 매뉴얼을 그대로 표시합니다.")
    elif top:
        guide = manual_guide(top)
        source = "manual"
    else:
        guide = fallback_guide()
        source = "fallback"
        notices.append("등록 매뉴얼에 없는 증상입니다. 부품 교체 절차는 만들지 않았습니다.")

    symptom_names = {normalize(top["error_code"]), normalize(top["title"])} if top else set()
    compact_query = normalize(body.query)
    exact_symptom = bool(top and (compact_query in symptom_names or any(name and name in compact_query for name in symptom_names)))
    if len(ranked) > 1 and not exact_symptom:
        notices.append("비슷한 현장 항목이 있어 가장 가까운 항목을 기준으로 안내합니다.")
    positioned = "{위치}" in "\n".join(guide.values())
    if positioned:
        for key in ("cause", "inspection", "action"):
            guide[key] = apply_position_name(guide[key], body.position_code)
    elif body.position_code and model_dict["code"] in {"GATE", "B650"}:
        guide["inspection"] = position_line(body.position_code) + "\n" + guide["inspection"]

    record = {
        "model_id": model_dict["id"],
        "model_name": history_label(body.system_code, model_dict, body.position_code),
        "system_code": body.system_code,
        "position_code": body.position_code,
        "position_label": position_label(body.position_code),
        "query": body.query,
        "cause": guide["cause"],
        "inspection": guide["inspection"],
        "action": guide["action"],
        "source": source,
        "matched_code": top["error_code"] if top else "",
        "matched_title": top["title"] if top else "",
        "manual_id": top["id"] if top else None,
        "notice": " ".join(notices),
    }
    with db() as conn:
        history_id, created_at = save_history(conn, record)

    return {
        **record,
        "history_id": history_id,
        "created_at": created_at,
        "related": related_payload(ranked),
    }


@app.get("/api/history")
def list_history(
    q: str = Query(default="", max_length=200),
    model_id: Optional[int] = None,
    limit: int = Query(default=100, ge=1, le=200),
) -> list[dict]:
    sql = "SELECT * FROM history WHERE 1 = 1"
    params: list[object] = []
    if model_id:
        sql += " AND model_id = ?"
        params.append(model_id)
    if q.strip():
        pat = like_pattern(q.strip())
        sql += """
            AND (
                query LIKE ? ESCAPE '\\'
                OR cause LIKE ? ESCAPE '\\'
                OR model_name LIKE ? ESCAPE '\\'
                OR matched_code LIKE ? ESCAPE '\\'
            )
        """
        params.extend([pat, pat, pat, pat])
    sql += " ORDER BY id DESC LIMIT ?"
    params.append(limit)
    with db() as conn:
        return [dict(row) for row in conn.execute(sql, params).fetchall()]


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
