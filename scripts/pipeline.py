"""
한일 보험 법령 파이프라인 v3
- JP: 18개 법령 (phase1 8개 + phase2 7개 + phase3 세법 3개)
- KR: 18개 법령 (phase1 8개 + phase2 7개 + phase3 세법 3개)
  ※ 행정규칙(KR-05,06,09,10,13,26)은 scripts/crawler.py 에서 별도 수집

실행: python scripts/pipeline.py --country ALL            # 전체
      python scripts/pipeline.py --country JP --phase 1  # 1단계만
      python scripts/pipeline.py --country KR --phase 3  # 세법만
"""

import os, re, time, json, logging, argparse
import xml.etree.ElementTree as ET
import requests
from supabase import create_client

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

SUPABASE_URL   = os.environ["SUPABASE_URL"]
SUPABASE_KEY   = os.environ["SUPABASE_SERVICE_KEY"]
GEMINI_API_KEY = os.environ["GEMINI_API_KEY"]
HOUREI_API_KEY = os.environ.get("HOUREI_API_KEY", "")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ══════════════════════════════════════════════════════════════════════════════
# 법령 정의
# ══════════════════════════════════════════════════════════════════════════════

JP_LAWS = [
    # ── phase 1: 보험·금융서비스·자본시장 핵심법령 ──────────────────────────
    {"id":"JP-01","phase":1,"law_code":"420AC0000000056",
     "name_ja":"保険法","name_ko":"보험법",
     "category":["보험사","소비자보호"],"tax_filter":False},
    {"id":"JP-02","phase":1,"law_code":"407AC0000000105",
     "name_ja":"保険業法","name_ko":"보험업법",
     "category":["보험사","허가·감독","보험대리점"],"tax_filter":False},
    {"id":"JP-03","phase":1,"law_code":"407CO0000000425",
     "name_ja":"保険業法施行令","name_ko":"보험업법 시행령",
     "category":["보험사","허가·감독"],"tax_filter":False},
    {"id":"JP-04","phase":1,"law_code":"408M50000040005",
     "name_ja":"保険業法施行規則","name_ko":"보험업법 시행규칙",
     "category":["보험사","허가·감독","보험대리점"],"tax_filter":False},
    {"id":"JP-05","phase":1,"law_code":"412AC0000000101",
     "name_ja":"金融サービスの提供及び利用環境の整備等に関する法律",
     "name_ko":"금융서비스제공법",
     "category":["소비자보호","인슈어테크"],"tax_filter":False},
    {"id":"JP-06","phase":1,"law_code":"412CO0000000484",
     "name_ja":"金融サービスの提供及び利用環境の整備等に関する法律施行令",
     "name_ko":"금융서비스제공법 시행령",
     "category":["소비자보호","인슈어테크"],"tax_filter":False},
    {"id":"JP-07","phase":1,"law_code":"323AC0000000025",
     "name_ja":"金融商品取引法","name_ko":"금융상품거래법",
     "category":["인슈어테크","소비자보호"],"tax_filter":False},
    {"id":"JP-08","phase":1,"law_code":"340CO0000000321",
     "name_ja":"金融商品取引法施行令","name_ko":"금융상품거래법 시행령",
     "category":["인슈어테크","소비자보호"],"tax_filter":False},

    # ── phase 2: 특별법·퇴직연금·방카슈랑스 ────────────────────────────────
    {"id":"JP-16","phase":2,"law_code":"330AC0000000097",
     "name_ja":"自動車損害賠償保障法","name_ko":"자동차손해배상보장법",
     "category":["보험사","의무보험"],"tax_filter":False},
    {"id":"JP-17","phase":2,"law_code":"330CO0000000286",
     "name_ja":"自動車損害賠償保障法施行令","name_ko":"자동차손해배상보장법 시행령",
     "category":["보험사","의무보험"],"tax_filter":False},
    {"id":"JP-18","phase":2,"law_code":"341AC0000000073",
     "name_ja":"地震保険に関する法律","name_ko":"지진보험법",
     "category":["보험사","손해보험"],"tax_filter":False},
    {"id":"JP-19","phase":2,"law_code":"341CO0000000164",
     "name_ja":"地震保険に関する法律施行令","name_ko":"지진보험법 시행령",
     "category":["보험사","손해보험"],"tax_filter":False},
    {"id":"JP-20","phase":2,"law_code":"413AC0000000088",
     "name_ja":"確定拠出年金法","name_ko":"확정갹출연금법",
     "category":["보험사","퇴직연금"],"tax_filter":False},
    {"id":"JP-21","phase":2,"law_code":"413AC0000000050",
     "name_ja":"確定給付企業年金法","name_ko":"확정급부기업연금법",
     "category":["보험사","퇴직연금"],"tax_filter":False},
    {"id":"JP-25","phase":2,"law_code":"356AC0000000059",
     "name_ja":"銀行法","name_ko":"은행법",
     "category":["방카슈랑스"],"tax_filter":False},

    # ── phase 3: 세법 (보험관련 조문 선별) ──────────────────────────────────
    # 昭和40年法律第33号
    {"id":"JP-22","phase":3,"law_code":"340AC0000000033",
     "name_ja":"所得税法","name_ko":"소득세법 (보험관련)",
     "category":["세금·세제"],"tax_filter":True,
     "tax_keywords":["保険","共済","年金","生命","損害","退職"]},
    # 昭和25年法律第73号
    {"id":"JP-23","phase":3,"law_code":"325AC0000000073",
     "name_ja":"相続税法","name_ko":"상속세법 (보험관련)",
     "category":["세금·세제"],"tax_filter":True,
     "tax_keywords":["保険","共済","年金","生命","みなし相続"]},
    # 昭和40年法律第34号
    {"id":"JP-24","phase":3,"law_code":"340AC0000000034",
     "name_ja":"法人税法","name_ko":"법인세법 (보험관련)",
     "category":["세금·세제"],"tax_filter":True,
     "tax_keywords":["保険","共済","損金","生命","退職給付"]},
]

KR_LAWS = [
    # ── phase 1: 보험·금융소비자보호·자본시장 핵심법령 ──────────────────────
    {"id":"KR-01","phase":1,"law_code":"284143",
     "name_ja":"","name_ko":"상법 (보험편)",
     "category":["보험사","소비자보호"],"tax_filter":False},
    {"id":"KR-02","phase":1,"law_code":"265389",
     "name_ja":"","name_ko":"보험업법",
     "category":["보험사","허가·감독","보험대리점"],"tax_filter":False},
    {"id":"KR-03","phase":1,"law_code":"279285",
     "name_ja":"","name_ko":"보험업법 시행령",
     "category":["보험사","허가·감독"],"tax_filter":False},
    {"id":"KR-04","phase":1,"law_code":"269221",
     "name_ja":"","name_ko":"보험업법 시행규칙",
     "category":["보험사","허가·감독"],"tax_filter":False},
    {"id":"KR-07","phase":1,"law_code":"277247",
     "name_ja":"","name_ko":"금융소비자보호에 관한 법률",
     "category":["소비자보호","보험대리점"],"tax_filter":False},
    {"id":"KR-08","phase":1,"law_code":"281375",
     "name_ja":"","name_ko":"금융소비자보호에 관한 법률 시행령",
     "category":["소비자보호","보험대리점"],"tax_filter":False},
    {"id":"KR-11","phase":1,"law_code":"284145",
     "name_ja":"","name_ko":"자본시장과 금융투자업에 관한 법률",
     "category":["인슈어테크","소비자보호"],"tax_filter":False},
    {"id":"KR-12","phase":1,"law_code":"281857",
     "name_ja":"","name_ko":"자본시장과 금융투자업에 관한 법률 시행령",
     "category":["인슈어테크","소비자보호"],"tax_filter":False},

    # ── phase 2: 특별법·퇴직연금·방카슈랑스 ────────────────────────────────
    {"id":"KR-16","phase":2,"law_code":"277017",
     "name_ja":"","name_ko":"자동차손해배상 보장법",
     "category":["보험사","의무보험"],"tax_filter":False},
    {"id":"KR-17","phase":2,"law_code":"272517",
     "name_ja":"","name_ko":"자동차손해배상 보장법 시행령",
     "category":["보험사","의무보험"],"tax_filter":False},
    {"id":"KR-18","phase":2,"law_code":"248933",
     "name_ja":"","name_ko":"화재로 인한 재해보상과 보험가입에 관한 법률",
     "category":["보험사","손해보험"],"tax_filter":False},
    {"id":"KR-19","phase":2,"law_code":"279829",
     "name_ja":"","name_ko":"근로자퇴직급여 보장법",
     "category":["보험사","퇴직연금"],"tax_filter":False},
    {"id":"KR-20","phase":2,"law_code":"262801",
     "name_ja":"","name_ko":"근로자퇴직급여 보장법 시행령",
     "category":["보험사","퇴직연금"],"tax_filter":False},
    {"id":"KR-24","phase":2,"law_code":"248929",
     "name_ja":"","name_ko":"은행법",
     "category":["방카슈랑스"],"tax_filter":False},
    {"id":"KR-25","phase":2,"law_code":"265787",
     "name_ja":"","name_ko":"우체국예금·보험에 관한 법률",
     "category":["보험사","공제"],"tax_filter":False},

    # ── phase 3: 세법 (보험관련 조문 선별) ──────────────────────────────────
    # lsId=001565 → MST=1565
    {"id":"KR-21","phase":3,"law_code":"247467",
     "name_ja":"","name_ko":"소득세법 (보험관련)",
     "category":["세금·세제"],"tax_filter":True,
     "tax_keywords":["보험료","보험금","공제","연금","퇴직소득","비과세"]},
    # lsId=001561 → MST=1561
    {"id":"KR-22","phase":3,"law_code":"276123",
     "name_ja":"","name_ko":"상속세및증여세법 (보험관련)",
     "category":["세금·세제"],"tax_filter":True,
     "tax_keywords":["보험금","보험료","연금","퇴직금","간주"]},
    # lsId=003608 → MST=3608
    {"id":"KR-23","phase":3,"law_code":"109644",
     "name_ja":"","name_ko":"법인세법 (보험관련)",
     "category":["세금·세제"],"tax_filter":True,
     "tax_keywords":["보험료","보험금","손금","퇴직급여","충당금"]},
]

# ══════════════════════════════════════════════════════════════════════════════
# 데이터 수집 함수
# ══════════════════════════════════════════════════════════════════════════════

EGOV_BASE    = "https://laws.e-gov.go.jp/api/1"
HOUREI_BASE  = "https://www.law.go.kr/DRF/lawService.do"

def _kanji_to_int(s):
    """第二十五条 → 25, 第一条の二 → (1,2) のような変換 (ソート用タプル)"""
    import re as _re
    M = {"一":1,"二":2,"三":3,"四":4,"五":5,"六":6,"七":7,"八":8,"九":9}
    def _ki(t):
        t = _re.sub(r"[第条項]","",t).strip()
        if not t: return 0
        m = _re.search(r"\d+", t)
        if m: return int(m.group())
        r = 0
        if "百" in t:
            p = t.split("百"); r += (M.get(p[0],1) if p[0] else 1)*100; t = p[1] if len(p)>1 else ""
        if "十" in t:
            p = t.split("十"); r += (M.get(p[0],1) if p[0] else 1)*10; t = p[1] if len(p)>1 else ""
        if t: r += M.get(t[0], 0)
        return r
    s = s or ""
    if "の" in s:
        p = s.split("の")
        return (_ki(p[0]), _ki(p[1] if len(p)>1 else ""))
    return (_ki(s), 0)

def fetch_jp_law(law: dict) -> list[dict]:
    url = f"{EGOV_BASE}/lawdata/{law['law_code']}"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    root = ET.fromstring(r.text)

    articles = []
    seen_ids = set()
    for art in root.iter("Article"):
        # Num 속성으로 고유 ID 생성 (第一条の二 → "1_2" 형태)
        num_attr = art.get("Num", "")          # e.g. "1", "1_2", "52_29"
        num_title = art.findtext("ArticleTitle") or ""  # e.g. 第一条, 第一条の二
        title = art.findtext("ArticleCaption") or ""

        # 본문 텍스트: ArticleTitle/ArticleCaption 제외하고 Sentence만
        sentences = []
        for elem in art.iter():
            if elem.tag == "Sentence" and elem.text:
                sentences.append(elem.text.strip())
        text = " ".join(sentences)
        if not text or len(text) < 5:
            # Sentence 없으면 전체 텍스트 fallback
            text = " ".join(t.strip() for t in art.itertext() if t.strip())
        if not text or len(text) < 5:
            continue
        if law.get("tax_filter") and not any(kw in text for kw in law.get("tax_keywords", [])):
            continue

        # 고유 ID: Num 속성 기반 (없으면 ArticleTitle의 숫자들)
        if num_attr:
            art_id = f"{law['id']}-{num_attr}"
        else:
            digits = re.sub(r'[^0-9_]', '', num_title.replace('の', '_'))
            art_id = f"{law['id']}-{digits or num_title}"

        # 중복 ID 처리
        if art_id in seen_ids:
            art_id = f"{art_id}-dup{len(seen_ids)}"
        seen_ids.add(art_id)

        articles.append({
            "id":       art_id,
            "law_id":   law["id"],
            "article":  num_title or num_attr,
            "title":    title,
            "text":     text[:2000],
            "keywords": _extract_keywords(text),
            "category": law["category"],
        })
    articles.sort(key=lambda a: _kanji_to_int(a["article"]))
    log.info(f"  JP {law['id']} ({law['name_ja']}): {len(articles)}개 조문")
    return articles

def fetch_kr_law(law: dict) -> list[dict]:
    # lsiSeq 파라미터로 법령 조회
    params = {"OC": HOUREI_API_KEY, "target": "law", "type": "JSON", "lsiSeq": law["law_code"]}
    r = requests.get(HOUREI_BASE, params=params, timeout=60)
    r.raise_for_status()
    try:
        data = r.json()
    except Exception as e:
        log.error(f"  KR {law['id']} JSON 파싱 실패: {e} / 응답: {r.text[:200]}")
        return []
    # 응답 최상위 키 로그
    top_keys = list(data.keys()) if isinstance(data, dict) else type(data).__name__
    log.info(f"  KR {law['id']} API 응답 최상위 키: {top_keys}")
    # 오류 응답 확인
    if isinstance(data, dict) and data.get("result"):
        log.error(f"  KR {law['id']} API 오류: {data}")
        return []

    jo_list = data.get("법령", {}).get("조문", {})
    if isinstance(jo_list, dict):
        jo_list = jo_list.get("조문단위", [])
    if isinstance(jo_list, dict):
        jo_list = [jo_list]
    if not isinstance(jo_list, list):
        jo_list = []

    articles = []
    for art in jo_list:
        num   = art.get("조문번호", "")
        title = art.get("조문제목", "")
        text  = art.get("조문내용", "")
        hang  = art.get("항", [])
        if isinstance(hang, dict): hang = [hang]
        for h in hang:
            text += " " + h.get("항내용", "")
        if not text.strip():
            continue
        if law.get("tax_filter") and not any(kw in text for kw in law.get("tax_keywords", [])):
            continue
        # KR-01 상법: 보험편(제638조~제739조)만 추출
        if law["id"] == "KR-01":
            art_num = int(re.sub(r'[^0-9]', '', str(num)) or "0")
            if not (638 <= art_num <= 739):
                continue
        articles.append({
            "id":       f"{law['id']}-art{re.sub(r'[^0-9]', '', str(num))}",
            "law_id":   law["id"],
            "article":  str(num),
            "title":    title,
            "text":     text.strip()[:2000],
            "keywords": _extract_keywords(text),
            "category": law["category"],
        })
    # 조번호 숫자 정렬
    articles.sort(key=lambda a: int(re.sub(r"[^0-9]","",a["article"]) or "0"))
    log.info(f"  KR {law['id']} ({law['name_ko']}): {len(articles)}개 조문")
    return articles

# ══════════════════════════════════════════════════════════════════════════════
# 청크 분할 / 임베딩 / Supabase
# ══════════════════════════════════════════════════════════════════════════════

def split_chunks(articles: list[dict]) -> list[dict]:
    chunks = []
    seen = set()
    for art in articles:
        base_id = art["id"]
        if len(art["text"]) <= 1500:
            # 중복 ID 방지
            uid = base_id
            if uid in seen:
                continue  # 완전 중복이면 스킵
            seen.add(uid)
            chunks.append(art)
        else:
            parts = re.split(r'(?=①|②|③|④|⑤|⑥|⑦|⑧|⑨|⑩|　一　|　二　)', art["text"])
            valid_parts = [p.strip() for p in parts if p.strip() and len(p.strip()) >= 10]
            if len(valid_parts) <= 1:
                # 분할이 안 되면 통으로 저장 (2000자로 자름)
                uid = base_id
                if uid not in seen:
                    seen.add(uid)
                    chunks.append({**art, "text": art["text"][:2000]})
            else:
                for i, para in enumerate(valid_parts):
                    uid = f"{base_id}-p{i+1}"
                    if uid not in seen:
                        seen.add(uid)
                        chunks.append({**art, "id": uid, "text": para[:2000]})
    log.info(f"  청크 분할: {len(articles)}개 → {len(chunks)}개")
    return chunks

EMBED_MODEL      = "text-embedding-004"
GEMINI_EMBED_URL = "https://generativelanguage.googleapis.com/v1/models/{model}:embedContent?key={key}"

def embed_single(text: str) -> list[float]:
    url  = GEMINI_EMBED_URL.format(model=EMBED_MODEL, key=GEMINI_API_KEY)
    body = {"model": f"models/{EMBED_MODEL}",
            "content": {"parts": [{"text": text}]}}
    r = requests.post(url, json=body, timeout=30)
    r.raise_for_status()
    return r.json()["embedding"]["values"]

def embed_chunks(chunks: list[dict]) -> list[dict]:
    for i, c in enumerate(chunks):
        try:
            c["embedding"] = embed_single(f"{c['title']} {c['text']}")
            if (i + 1) % 10 == 0:
                log.info(f"  임베딩: {i+1}/{len(chunks)}")
        except Exception as e:
            log.warning(f"  임베딩 실패 ({c['id']}): {e}")
            c["embedding"] = None
        time.sleep(0.5)
    return chunks

def upsert_laws(law_list: list[dict], country: str):
    rows = [{"id": l["id"], "name_ja": l.get("name_ja",""), "name_ko": l["name_ko"],
             "country": country, "category": l["category"],
             "law_code": l["law_code"], "phase": l["phase"]} for l in law_list]
    supabase.table("laws").upsert(rows).execute()
    log.info(f"  laws upsert: {len(rows)}건")

def upsert_chunks(chunks: list[dict]):
    seen = {}
    for c in chunks:
        seen[c["id"]] = c
    chunks = list(seen.values())
    log.info(f"  중복 제거 후: {len(chunks)}개")

    rows_emb    = [{"id":c["id"],"law_id":c["law_id"],"article":c["article"],
                    "title":c["title"],"text":c["text"],"keywords":c["keywords"],
                    "category":c["category"],"embedding":c["embedding"]}
                   for c in chunks if c.get("embedding") is not None]
    rows_no_emb = [{"id":c["id"],"law_id":c["law_id"],"article":c["article"],
                    "title":c["title"],"text":c["text"],"keywords":c["keywords"],
                    "category":c["category"]}
                   for c in chunks if c.get("embedding") is None]

    for i in range(0, len(rows_emb), 50):
        supabase.table("chunks").upsert(rows_emb[i:i+50]).execute()
        log.info(f"  chunks (emb): {min(i+50,len(rows_emb))}/{len(rows_emb)}")
    for i in range(0, len(rows_no_emb), 50):
        supabase.table("chunks").upsert(rows_no_emb[i:i+50]).execute()
    if rows_no_emb:
        log.info(f"  chunks (no emb): {len(rows_no_emb)}건")

def export_laws_index(law_list: list[dict], country: str):
    path = "docs/data/laws_index.json"
    existing = {}
    if os.path.exists(path):
        with open(path) as f:
            existing = {l["id"]: l for l in json.load(f).get("laws", [])}
    for l in law_list:
        existing[l["id"]] = {"id":l["id"],"name_ja":l.get("name_ja",""),
                              "name_ko":l["name_ko"],"country":country,
                              "category":l["category"],"phase":l["phase"]}
    os.makedirs("docs/data", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"updated_at": time.strftime("%Y-%m-%d"),
                   "laws": list(existing.values())}, f, ensure_ascii=False, indent=2)
    log.info("  laws_index.json 갱신")

def _extract_keywords(text: str) -> list[str]:
    words = re.findall(r'[ぁ-んァ-ン一-龥가-힣]{3,}', text)
    freq  = {}
    for w in words:
        freq[w] = freq.get(w, 0) + 1
    return [w for w, c in sorted(freq.items(), key=lambda x: -x[1]) if c >= 2][:10]

# ══════════════════════════════════════════════════════════════════════════════
# 메인
# ══════════════════════════════════════════════════════════════════════════════

def run(country: str, phase: int = 0):
    def fp(law_list):
        return law_list if phase == 0 else [l for l in law_list if l["phase"] == phase]

    if country in ("JP", "ALL"):
        target = fp(JP_LAWS)
        log.info(f"=== JP 수집 ({len(target)}개, phase={phase}) ===")
        upsert_laws(target, "JP")
        all_chunks = []
        for law in target:
            try:
                all_chunks.extend(split_chunks(fetch_jp_law(law)))
            except Exception as e:
                log.error(f"  {law['id']} 실패: {e}")
        if all_chunks:
            embed_chunks(all_chunks)
            upsert_chunks(all_chunks)
        export_laws_index(target, "JP")
        log.info(f"  JP 완료: {len(all_chunks)}개 청크")

    if country in ("KR", "ALL"):
        target = fp(KR_LAWS)
        log.info(f"=== KR 수집 ({len(target)}개, phase={phase}) ===")
        upsert_laws(target, "KR")
        all_chunks = []
        for law in target:
            try:
                all_chunks.extend(split_chunks(fetch_kr_law(law)))
            except Exception as e:
                log.error(f"  {law['id']} 실패: {e}")
        if all_chunks:
            embed_chunks(all_chunks)
            upsert_chunks(all_chunks)
        export_laws_index(target, "KR")
        log.info(f"  KR 완료: {len(all_chunks)}개 청크")

    log.info("=== 완료 ===")

def embed_only(country: str, batch: int = 100):
    """임베딩이 없는 기존 청크에만 임베딩을 채운다 (법령 재수집 없이)."""
    log.info(f"=== embed-only 모드: country={country}, batch={batch} ===")
    offset = 0
    total_updated = 0
    while True:
        q = supabase.table("chunks") \
            .select("id,law_id,title,text") \
            .is_("embedding", "null") \
            .limit(batch) \
            .offset(offset)
        if country != "ALL":
            # law_id 접두사로 필터 (JP-xx / KR-xx)
            q = q.like("law_id", f"{country}%")
        rows = q.execute().data
        if not rows:
            break
        log.info(f"  임베딩 대상: {len(rows)}개 (offset={offset})")
        for row in rows:
            try:
                emb = embed_single(f"{row.get('title','')} {row['text']}")
                supabase.table("chunks").update({"embedding": emb}).eq("id", row["id"]).execute()
                total_updated += 1
            except Exception as e:
                log.warning(f"  임베딩 실패 ({row['id']}): {e}")
            time.sleep(0.5)
        offset += batch
    log.info(f"=== embed-only 완료: {total_updated}개 업데이트 ===")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--country", choices=["JP","KR","ALL"], default="ALL")
    parser.add_argument("--phase", type=int, choices=[0,1,2,3], default=0,
                        help="0=전체, 1=핵심법령, 2=특별법·연금, 3=세법")
    parser.add_argument("--embed-only", action="store_true",
                        help="법령 재수집 없이 임베딩 없는 청크에만 임베딩 채우기")
    parser.add_argument("--batch", type=int, default=100,
                        help="embed-only 배치 크기 (기본 100)")
    args = parser.parse_args()

    if args.embed_only:
        embed_only(args.country, args.batch)
    else:
        run(args.country, args.phase)
