"""
FSA(금융청) 감독지침 + 법제처 행정규칙 크롤러 v2
- JP-09~15: FSA HTML 페이지 크롤링
- KR-05,06,09,10,13,26: 법제처 행정규칙 API (admRulSeq)

실행: python scripts/crawler.py --target JP
      python scripts/crawler.py --target KR
      python scripts/crawler.py --target ALL
"""

import os, re, time, json, logging, argparse
import requests
from bs4 import BeautifulSoup
from supabase import create_client

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

SUPABASE_URL   = os.environ["SUPABASE_URL"]
SUPABASE_KEY   = os.environ["SUPABASE_SERVICE_KEY"]
GEMINI_API_KEY = os.environ["GEMINI_API_KEY"]
HOUREI_API_KEY = os.environ.get("HOUREI_API_KEY", "")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ══════════════════════════════════════════════════════════════════════════════
# FSA 감독지침 정의 (JP-09 ~ JP-15)
# base_url: 목차 index가 있는 디렉토리
# pages: 수집할 HTML 파일 목록 (빈 리스트면 index에서 자동 수집)
# ══════════════════════════════════════════════════════════════════════════════

FSA_GUIDELINES = [
    {
        "id": "JP-09", "phase": 1,
        "name_ja": "保険会社向けの総合的な監督指針",
        "name_ko": "보험회사 감독지침",
        "category": ["보험사", "허가·감독"],
        "base_url": "https://www.fsa.go.jp/common/law/guide/ins/",
        # 목차에서 자동 수집 (index.html에서 NN.html 링크 파싱)
        "pages": [],
    },
    {
        "id": "JP-10", "phase": 1,
        "name_ja": "少額短期保険業者向けの監督指針",
        "name_ko": "소액단기보험업자 감독지침",
        "category": ["보험사", "허가·감독"],
        "base_url": "https://www.fsa.go.jp/common/law/guide/sas/",
        "pages": [],
    },
    {
        "id": "JP-11", "phase": 1,
        "name_ja": "認可特定保険業者向けの総合的な監督指針",
        "name_ko": "인가특정보험업자 감독지침",
        "category": ["보험사", "허가·감독"],
        "base_url": "https://www.fsa.go.jp/common/law/guide/nintoku/",
        "pages": [],
    },
    {
        "id": "JP-12", "phase": 1,
        "name_ja": "金融商品取引業者等向けの総合的な監督指針",
        "name_ko": "금융상품거래업자 감독지침",
        "category": ["인슈어테크", "소비자보호"],
        "base_url": "https://www.fsa.go.jp/common/law/guide/kinyushohin/",
        "pages": [],
    },
    {
        "id": "JP-13", "phase": 1,
        "name_ja": "金融サービス仲介業者向けの総合的な監督指針",
        "name_ko": "금융서비스중개업자 감독지침",
        "category": ["보험대리점", "소비자보호"],
        "base_url": "https://www.fsa.go.jp/common/law/guide/chukkai/",
        "pages": [],
    },
    {
        "id": "JP-14", "phase": 1,
        "name_ja": "事務ガイドライン第三分冊：金融会社関係",
        "name_ko": "사무가이드라인 제3분책",
        "category": ["보험사", "허가·감독"],
        "base_url": "https://www.fsa.go.jp/common/law/guide/03/",
        "pages": [],
    },
    {
        "id": "JP-15", "phase": 1,
        "name_ja": "告示・ガイドライン・Q&A・法令解釈事例集 (保険関連)",
        "name_ko": "고시·가이드라인·Q&A (보험)",
        "category": ["보험사", "소비자보호"],
        # 보험관련 고시·QA 목록 페이지
        "base_url": "https://www.fsa.go.jp/common/law/",
        "pages": ["ins_qa.html"],   # 존재할 경우 수집, 없으면 목차에서 자동
    },
]

# ══════════════════════════════════════════════════════════════════════════════
# 법제처 행정규칙 정의 (KR-05,06,09,10,13,26)
# adm_rule_seq: 법제처 admRulInfoP.do 페이지에서 직접 확인한 ID
# ══════════════════════════════════════════════════════════════════════════════

KR_ADM_RULES = [
    {
        "id": "KR-05", "phase": 1,
        "adm_rule_seq": "2100000272874",
        "name_ko": "보험업감독규정",
        "category": ["보험사", "허가·감독"],
    },
    {
        "id": "KR-06", "phase": 1,
        "adm_rule_seq": "2200000108593",
        "name_ko": "보험업감독업무시행세칙",
        "category": ["보험사", "허가·감독"],
    },
    {
        "id": "KR-09", "phase": 1,
        "adm_rule_seq": "2100000268650",
        "name_ko": "금융소비자보호에관한감독규정",
        "category": ["소비자보호", "보험대리점"],
    },
    {
        "id": "KR-10", "phase": 1,
        "adm_rule_seq": "2200000108171",
        "name_ko": "금융소비자보호에관한감독규정시행세칙",
        "category": ["소비자보호", "보험대리점"],
    },
    {
        "id": "KR-13", "phase": 1,
        "adm_rule_seq": "2100000275618",
        "name_ko": "금융투자업규정",
        "category": ["인슈어테크", "소비자보호"],
    },
    {
        "id": "KR-26", "phase": 1,
        "adm_rule_seq": "2100000272518",
        "name_ko": "금융기관검사및제재에관한규정",
        "category": ["보험사", "허가·감독"],
    },
]

# ══════════════════════════════════════════════════════════════════════════════
# FSA 크롤링
# ══════════════════════════════════════════════════════════════════════════════

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; InsuranceLawBot/1.0; +https://github.com/SeeTheSkyEric/Japan-Insurance-Law-Chatbot)"}

def _discover_pages(base_url: str) -> list[str]:
    """index.html에서 NN.html 형태의 링크를 자동 발견"""
    index_url = base_url + "index.html"
    try:
        r = requests.get(index_url, headers=HEADERS, timeout=30)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        found = set()
        for a in soup.find_all("a", href=True):
            href = a["href"]
            # NN.html 또는 ./NN.html 패턴
            m = re.match(r'^\.?/?(\d+\.html)$', href)
            if m:
                found.add(m.group(1))
        pages = sorted(found, key=lambda x: int(re.sub(r'\D', '', x) or '0'))
        log.info(f"    index 자동 발견: {pages}")
        return pages if pages else ["index.html"]
    except Exception as e:
        log.warning(f"    index.html 접근 실패 ({base_url}): {e}")
        return ["index.html"]

def _parse_fsa_page(url: str) -> list[dict]:
    """FSA HTML 페이지에서 섹션별 청크 추출"""
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    # FSA 페이지는 UTF-8이지만 일부는 Shift-JIS → chardet로 자동 감지
    r.encoding = r.apparent_encoding or "utf-8"
    soup = BeautifulSoup(r.text, "html.parser")

    # 본문 컨테이너 탐색
    main = (soup.find("div", id="main") or
            soup.find("div", class_="main-content") or
            soup.find("article") or
            soup.find("body"))
    if not main:
        return []

    chunks = []
    current_title = ""
    current_lines: list[str] = []

    def flush():
        text = " ".join(current_lines).strip()
        if text and len(text) > 20:
            chunks.append({"title": current_title, "text": text[:2000]})

    for elem in main.find_all(["h2", "h3", "h4", "p", "li"], recursive=True):
        if elem.name in ("h2", "h3", "h4"):
            flush()
            current_title = elem.get_text(" ", strip=True)
            current_lines = []
        else:
            t = elem.get_text(" ", strip=True)
            if t:
                current_lines.append(t)

    flush()
    return chunks

def fetch_fsa_guideline(law: dict) -> list[dict]:
    base_url = law["base_url"]
    pages = law["pages"] or _discover_pages(base_url)

    all_articles = []
    for page_idx, page in enumerate(pages):
        url = base_url + page
        try:
            page_chunks = _parse_fsa_page(url)
            for chunk_idx, chunk in enumerate(page_chunks):
                art_id = f"{law['id']}-p{page_idx+1}s{chunk_idx+1}"
                all_articles.append({
                    "id":       art_id,
                    "law_id":   law["id"],
                    "article":  f"p{page_idx+1}-s{chunk_idx+1}",
                    "title":    chunk["title"],
                    "text":     chunk["text"],
                    "keywords": _extract_keywords(chunk["text"]),
                    "category": law["category"],
                })
            log.info(f"  {law['id']} [{page}]: {len(page_chunks)}개 섹션")
        except Exception as e:
            log.error(f"  {law['id']} [{page}] 실패: {e}")
        time.sleep(1.0)  # FSA 서버 부하 방지

    log.info(f"  FSA {law['id']} ({law['name_ja']}): 총 {len(all_articles)}개 청크")
    return all_articles

# ══════════════════════════════════════════════════════════════════════════════
# 법제처 행정규칙 API
# ══════════════════════════════════════════════════════════════════════════════

ADM_RULE_URL = "https://www.law.go.kr/DRF/lawService.do"

def fetch_kr_adm_rule(law: dict) -> list[dict]:
    params = {
        "OC":        HOUREI_API_KEY,
        "target":    "admrul",
        "type":      "JSON",
        "admRulSeq": law["adm_rule_seq"],
    }
    r = requests.get(ADM_RULE_URL, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    jo_list = data.get("행정규칙", {}).get("조문", {})
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
        num_clean = re.sub(r'[^0-9]', '', str(num))
        articles.append({
            "id":       f"{law['id']}-art{num_clean or num}",
            "law_id":   law["id"],
            "article":  str(num),
            "title":    title,
            "text":     text.strip()[:2000],
            "keywords": _extract_keywords(text),
            "category": law["category"],
        })
    log.info(f"  KR ADM {law['id']} ({law['name_ko']}): {len(articles)}개 조문")
    return articles

# ══════════════════════════════════════════════════════════════════════════════
# 임베딩 / Supabase / 유틸
# ══════════════════════════════════════════════════════════════════════════════

EMBED_MODEL      = "text-embedding-004"
GEMINI_EMBED_URL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:embedContent?key={key}"

def embed_single(text: str) -> list[float]:
    url  = GEMINI_EMBED_URL.format(model=EMBED_MODEL, key=GEMINI_API_KEY)
    body = {"model": f"models/{EMBED_MODEL}",
            "content": {"parts": [{"text": text}]},
            "taskType": "RETRIEVAL_DOCUMENT"}
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

def upsert_laws_meta(law_list: list[dict], country: str, id_field: str = "id"):
    rows = [{"id": l["id"], "name_ja": l.get("name_ja", ""), "name_ko": l.get("name_ko", ""),
             "country": country, "category": l["category"],
             "law_code": l.get("adm_rule_seq", l.get("base_url", "")),
             "phase": l.get("phase", 1)} for l in law_list]
    supabase.table("laws").upsert(rows).execute()
    log.info(f"  laws upsert: {len(rows)}건")

def upsert_chunks(chunks: list[dict]):
    seen = {}
    for c in chunks:
        seen[c["id"]] = c
    chunks = list(seen.values())

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
    for i in range(0, len(rows_no_emb), 50):
        supabase.table("chunks").upsert(rows_no_emb[i:i+50]).execute()
    log.info(f"  chunks upsert 완료: emb={len(rows_emb)}, no_emb={len(rows_no_emb)}")

def export_laws_index(law_list: list[dict], country: str):
    path = "docs/data/laws_index.json"
    existing = {}
    if os.path.exists(path):
        with open(path) as f:
            existing = {l["id"]: l for l in json.load(f).get("laws", [])}
    for l in law_list:
        existing[l["id"]] = {"id": l["id"], "name_ja": l.get("name_ja",""),
                              "name_ko": l.get("name_ko",""), "country": country,
                              "category": l["category"], "phase": l.get("phase", 1)}
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

def run(target: str):
    if target in ("JP", "ALL"):
        log.info(f"=== FSA 감독지침 수집 ({len(FSA_GUIDELINES)}개) ===")
        upsert_laws_meta(
            [{**l, "name_ko": l["name_ko"]} for l in FSA_GUIDELINES], "JP"
        )
        all_chunks = []
        for law in FSA_GUIDELINES:
            try:
                all_chunks.extend(fetch_fsa_guideline(law))
            except Exception as e:
                log.error(f"  {law['id']} 실패: {e}")
        if all_chunks:
            embed_chunks(all_chunks)
            upsert_chunks(all_chunks)
        export_laws_index(FSA_GUIDELINES, "JP")
        log.info(f"  FSA 총 {len(all_chunks)}개 청크 완료")

    if target in ("KR", "ALL"):
        log.info(f"=== 법제처 행정규칙 수집 ({len(KR_ADM_RULES)}개) ===")
        upsert_laws_meta(KR_ADM_RULES, "KR")
        all_chunks = []
        for law in KR_ADM_RULES:
            try:
                all_chunks.extend(fetch_kr_adm_rule(law))
            except Exception as e:
                log.error(f"  {law['id']} 실패: {e}")
        if all_chunks:
            embed_chunks(all_chunks)
            upsert_chunks(all_chunks)
        export_laws_index(KR_ADM_RULES, "KR")
        log.info(f"  행정규칙 총 {len(all_chunks)}개 청크 완료")

    log.info("=== 완료 ===")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", choices=["JP", "KR", "ALL"], default="ALL")
    args = parser.parse_args()
    run(args.target)
