import os
import re
import json
import asyncio
import requests
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import ApplicationBuilder, ContextTypes, CommandHandler, MessageHandler, filters
from telegram.constants import ChatAction
 
# ─── 1. LOAD CONFIGURATION ────────────────────────────────────────────────────
load_dotenv()
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
DATABASE_URL       = os.getenv("DATABASE_URL", "postgresql://user:password@host:5432/dbname")
DINOIKI_API_KEY    = os.getenv("DINOIKI_API_KEY", "")
DINOIKI_URL        = "https://ai.dinoiki.com/v1/chat/completions"
AI_MODEL           = "gpt-4o"

# ─── 2. STOPWORDS ─────────────────────────────────────────────────────────────
STOPWORDS = {
    'apa', 'siapa', 'berapa', 'yang', 'dan', 'atau', 'dari', 'untuk',
    'dengan', 'adalah', 'ini', 'itu', 'ada', 'tidak', 'bisa', 'mau',
    'saya', 'kamu', 'dia', 'kami', 'kita', 'mereka', 'semua', 'sudah',
    'belum', 'sedang', 'akan', 'telah', 'pada', 'di', 'ke', 'oleh',
    'juga', 'hanya', 'lebih', 'paling', 'sangat', 'banyak', 'sedikit',
    'tampilkan', 'tunjukkan', 'cari', 'lihat', 'data', 'info', 'informasi',
    'list', 'daftar', 'total', 'jumlah', 'nilai', 'status', 'semua',
    'kontrak', 'vendor', 'tagihan', 'dokumen', 'progress', 'bulan', 'tahun'
}

# ─── 3. DATABASE SCHEMA CONTEXT ───────────────────────────────────────────────
SCHEMA_CONTEXT = """
Database PostgreSQL untuk sistem manajemen kontrak kilang minyak. Berikut skema tabel:

TABEL: profiles
Kolom: id, email, full_name, role (admin/pic/user), password_hash, created_at, updated_at, is_active, id_vendor

TABEL: vendor
Kolom: id_vendor, nama_vendor, npwp, alamat, pic_nama, pic_kontak, status_vendor (Active/Inactive/Blacklist), score, created_at, updated_at

TABEL: kontrak
Kolom: id_kontrak, id_vendor, judul_kontrak, no_dokumen_kontrak, no_po_pr, direksi_pekerjaan,
  tipe_kontrak (Lumpsum/Unit Price/TSA/LTSA/TSA-LTSA), status_kontrak (Pre-KOM/Aktif/Selesai/Terminated),
  tanggal_spb_diterima, tanggal_terima_dokumen, tanggal_maksimal_kom, tanggal_mulai, tanggal_selesai,
  sla_kom_hari, estimasi_tanggal_kom, tanggal_kom, kom_terlambat, nilai_awal, durasi_kontrak_hari,
  progress_plan, progress_actual, aktivitas_saat_ini, kendala, disiplin, tkdn_percentage, tanggal_lkp,
  has_amendment, no_amandemen, tanggal_amandemen, jenis_amandemen, nilai_kontrak_baru, durasi_amandemen,
  tanggal_mulai_baru, tanggal_selesai_baru, alasan_perubahan, tanggal_mpl, tanggal_mpa,
  masa_pemeliharaan_hari, created_at, updated_at

TABEL: amandemen_kontrak
Kolom: id_amandemen, id_kontrak, nomor_urut, no_amandemen, tanggal_amandemen, jenis_amandemen,
  nilai_kontrak_baru, durasi_amandemen, tanggal_mulai_baru, tanggal_selesai_baru, alasan_perubahan,
  created_at, updated_at

TABEL: tagihan
Kolom: id_tagihan, id_kontrak, nomor_tagihan, tanggal_tagihan, tipe_kontrak, termin, nilai_tagihan,
  status_tagihan, memo_required, tanggal_pengiriman_memo, catatan, created_at, updated_at

TABEL: progress_lumpsum
Kolom: id_progress, id_kontrak, milestone, persen, tanggal_update, created_at

TABEL: progress_unit_price
Kolom: id_progress, id_kontrak, nama_item, satuan, qty_rencana, qty_aktual, harga_satuan, tanggal_update, created_at

TABEL: monitoring_ltsa
Kolom: id_log, id_kontrak, tanggal_kunjungan, jenis_layanan (Preventive/Corrective/Standby),
  durasi_jam, sla_terpenuhi (Yes/No), keterangan, created_at

TABEL: padi
Kolom: id_padi, no_pembelian, tanggal, judul_pembelian, no_po_pr, nilai, id_vendor, link_pembelian,
  bagian, status_purchase (BAST), tanggal_bast, tanggal_sa_gr, tanggal_invoice,
  tanggal_payment_approval, tanggal_paid, catatan_status, created_at, updated_at

TABEL: dokumen_approval
Kolom: id_dokumen, id_kontrak, tipe_dokumen (Evident/Report/Persetujuan), nama_dokumen,
  deskripsi_dokumen, status_approval (Pending/Approved/Rejected), catatan_reviewer,
  uploaded_by, reviewed_by, reviewed_at, created_at, updated_at

TABEL: daily_report
Kolom: id_report, tanggal_laporan, disiplin (Electrical/Instrument/Rotating/Stationary/Alat Berat),
  direksi (MA5/MA6/MA7/Workshop), kategori (Corrective Maintenance/Preventive Maintenance/Plant Patrol/Progress/Challenge Session),
  tag_number, deskripsi, status_pekerjaan (Done/In Progress/Waiting Material/Pending/-),
  catatan, pengirim_wa, raw_text, created_at

Relasi penting:
- vendor.id_vendor -> kontrak.id_vendor (1 vendor banyak kontrak)
- kontrak.id_kontrak -> tagihan.id_kontrak
- kontrak.id_kontrak -> amandemen_kontrak.id_kontrak
- kontrak.id_kontrak -> progress_lumpsum.id_kontrak
- kontrak.id_kontrak -> progress_unit_price.id_kontrak
- kontrak.id_kontrak -> monitoring_ltsa.id_kontrak
- kontrak.id_kontrak -> dokumen_approval.id_kontrak
- vendor.id_vendor -> padi.id_vendor

NILAI ENUM & PILIHAN YANG VALID:

1. TIPE KONTRAK: 'Lumpsum', 'Unit Price', 'TSA', 'LTSA', 'TSA/LTSA'
2. STATUS KONTRAK: 'Pre-KOM', 'Aktif', 'Selesai', 'Terminated'
3. DISIPLIN: 'Instrumentasi', 'Stationary', 'Electrical', 'Rotating', 'Alat Berat'
4. DIREKSI PEKERJAAN: 'MA5', 'MA6', 'MA7', 'Workshop'
5. JENIS AMANDEMEN: 'Nilai', 'Waktu', 'Nilai dan Waktu'
6. STATUS APPROVAL: 'Pending', 'Approved', 'Rejected'
7. STATUS VENDOR: 'Active', 'Inactive', 'Blacklist'
8. JENIS LAYANAN LTSA: 'Preventive', 'Corrective', 'Standby'
9. STATUS TAGIHAN (urutan tahapan):
   LKP -> Punchlist -> BAST -> BAKP/BAPP -> Submit i-Vendor -> SA -> PA -> Verification -> Payment/Selesai
"""

# ─── 4. SYSTEM PROMPT ─────────────────────────────────────────────────────────
BASE_SYSTEM_PROMPT = (
    "Kamu adalah asisten cerdas untuk sistem manajemen kontrak kilang minyak, "
    "yang menjawab pertanyaan via Telegram.\n"
    "Kamu dapat menjawab pertanyaan bisnis dalam Bahasa Indonesia secara natural "
    "dan mengkonversinya ke query SQL PostgreSQL.\n\n"
    + SCHEMA_CONTEXT +
    "\nATURAN QUERY SQL:\n"
    "1. HANYA boleh generate query SELECT — TIDAK boleh UPDATE, DELETE, INSERT, DROP, ALTER, TRUNCATE\n"
    "2. TIDAK boleh SELECT * — selalu tentukan kolom yang relevan\n"
    "3. Selalu gunakan LIMIT maksimal 50 baris\n"
    "4. Gunakan JOIN yang tepat antar tabel\n"
    "5. Format angka nilai kontrak dalam format Indonesia (Rp)\n"
    "\nATURAN INTERPRETASI ENTITAS:\n"
    "- Jika ada blok 'KONTEKS ENTITAS YANG DITEMUKAN DI DATABASE' -> gunakan langsung, JANGAN minta klarifikasi\n"
    "- Jika user menyebut nama yang diawali PT/CV/UD -> cari di vendor.nama_vendor\n"
    "- Jika user menyebut kode seperti MA5, KOM-001 -> cari di direksi_pekerjaan atau no_dokumen_kontrak\n"
    "- Jika entitas tidak ditemukan di konteks -> baru boleh minta klarifikasi\n"
    "\nATURAN FORMAT JAWABAN (KHUSUS TELEGRAM — NARASI SAJA):\n"
    "1. JAWABAN FULL NARASI — JANGAN gunakan tabel HTML, JANGAN format markdown [CHART]\n"
    "2. Jika hasil lebih dari 10 item, tampilkan ringkasan/highlight saja, maksimal 5-7 poin\n"
    "3. Gunakan poin-poin dengan tanda • jika data lebih dari satu\n"
    "4. Tebalkan poin penting dengan *teks* (bold Telegram)\n"
    "5. Tambahkan emoticon relevan (📋, 💰, 📊, ✅, ⚠️, 🔧, 🏭, 🚨, 🔴, 🟢)\n"
    "6. Gunakan angka dengan format mudah dibaca (contoh: Rp 1.250.000.000 atau Rp 1,25 M)\n"
    "7. Jika data panjang, akhiri dengan: _(Menampilkan highlight, tanya lebih spesifik untuk detail)_\n"
    "8. DETEKSI PERTANYAAN TIDAK PRODUKTIF:\n"
    "   - 'tampilkan semua', 'list semua', 'dump data' -> tolak sopan, minta pertanyaan lebih spesifik\n"
    "   - Pertanyaan di luar konteks monitoring kontrak -> jawab: 'Maaf, saya hanya membantu analisis data kontrak kilang.'\n"
    "   PENGECUALIAN — tetap jawab ramah untuk:\n"
    "   * Sapaan (halo, selamat pagi, dsb) -> balas ramah\n"
    "   * Tanya kemampuan AI -> jelaskan apa saja yang bisa dibantu\n"
    "   * Ucapan terima kasih -> balas sopan\n"
    "\nFORMAT RESPONS JSON:\n"
    "Kamu HARUS selalu merespons dalam format JSON seperti ini:\n"
    "{\n"
    '  "type": "query" | "clarification" | "narrative" | "error",\n'
    '  "sql": "query SQL jika type=query, null jika tidak",\n'
    '  "explanation": "penjelasan singkat apa yang akan dilakukan",\n'
    '  "narrative_hint": "bagaimana cara menarasikan hasilnya",\n'
    '  "clarification_question": "pertanyaan klarifikasi jika type=clarification",\n'
    '  "message": "pesan untuk user dalam format Telegram"\n'
    "}\n"
)

# ─── 5. HELPER: CALL AI ────────────────────────────────────────────────────────
def call_ai(messages: list, max_tokens: int = 1500) -> str:
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {DINOIKI_API_KEY}"
    }
    payload = {
        "model": AI_MODEL,
        "messages": messages,
        "max_tokens": max_tokens,
        "temperature": 0.3,
    }
    resp = requests.post(DINOIKI_URL, headers=headers, json=payload, timeout=60)
    resp.raise_for_status()
    return resp.json()["choices"][0]["message"]["content"].strip()

# ─── 6. SMART ENTITY SEARCH ───────────────────────────────────────────────────
def smart_entity_search(user_message: str) -> str:
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur  = conn.cursor()
        context   = []
        found_ids = set()

        words = [w for w in re.findall(r'\b\w{3,}\b', user_message)
                 if w.lower() not in STOPWORDS]
        company_patterns = re.findall(
            r'\b(?:PT|CV|UD|TB|PD)\s+[\w\s]+', user_message, re.IGNORECASE
        )
        search_terms = list(set(words + company_patterns))

        for term in search_terms:
            term = term.strip()
            if len(term) < 3:
                continue

            cur.execute("""
                SELECT id_vendor, nama_vendor, status_vendor, score
                FROM vendor
                WHERE nama_vendor ILIKE %s
                LIMIT 3
            """, (f'%{term}%',))
            for v in cur.fetchall():
                key = f"vendor_{v[0]}"
                if key not in found_ids:
                    found_ids.add(key)
                    context.append(
                        f"[VENDOR] '{v[1]}' -> id_vendor={v[0]}, "
                        f"status={v[2]}, score={v[3]}"
                    )

            cur.execute("""
                SELECT k.id_kontrak, k.judul_kontrak, k.no_dokumen_kontrak,
                       k.direksi_pekerjaan, k.status_kontrak, k.tipe_kontrak,
                       v.nama_vendor
                FROM kontrak k
                LEFT JOIN vendor v ON k.id_vendor = v.id_vendor
                WHERE k.judul_kontrak ILIKE %s
                   OR k.no_dokumen_kontrak ILIKE %s
                   OR k.no_po_pr ILIKE %s
                   OR k.direksi_pekerjaan ILIKE %s
                LIMIT 3
            """, (f'%{term}%', f'%{term}%', f'%{term}%', f'%{term}%'))
            for k in cur.fetchall():
                key = f"kontrak_{k[0]}"
                if key not in found_ids:
                    found_ids.add(key)
                    context.append(
                        f"[KONTRAK] '{k[1]}' -> id_kontrak={k[0]}, "
                        f"doc={k[2]}, direksi={k[3]}, "
                        f"status={k[4]}, tipe={k[5]}, vendor='{k[6]}'"
                    )

            cur.execute("""
                SELECT t.id_tagihan, t.nomor_tagihan, t.status_tagihan,
                       t.nilai_tagihan, k.judul_kontrak
                FROM tagihan t
                LEFT JOIN kontrak k ON t.id_kontrak = k.id_kontrak
                WHERE t.nomor_tagihan ILIKE %s
                LIMIT 3
            """, (f'%{term}%',))
            for t in cur.fetchall():
                key = f"tagihan_{t[0]}"
                if key not in found_ids:
                    found_ids.add(key)
                    context.append(
                        f"[TAGIHAN] '{t[1]}' -> id_tagihan={t[0]}, "
                        f"status={t[2]}, nilai={t[3]}, kontrak='{t[4]}'"
                    )

            cur.execute("""
                SELECT p.id_padi, p.no_pembelian, p.judul_pembelian,
                       p.nilai, v.nama_vendor
                FROM padi p
                LEFT JOIN vendor v ON p.id_vendor = v.id_vendor
                WHERE p.no_pembelian ILIKE %s
                   OR p.judul_pembelian ILIKE %s
                LIMIT 3
            """, (f'%{term}%', f'%{term}%'))
            for p in cur.fetchall():
                key = f"padi_{p[0]}"
                if key not in found_ids:
                    found_ids.add(key)
                    context.append(
                        f"[PADI] '{p[2]}' -> id_padi={p[0]}, "
                        f"no_pembelian={p[1]}, nilai={p[3]}, vendor='{p[4]}'"
                    )

        conn.close()

        if context:
            result  = "\n\nKONTEKS ENTITAS YANG DITEMUKAN DI DATABASE:\n"
            result += "(Gunakan informasi ini untuk memahami maksud user tanpa perlu klarifikasi)\n"
            result += "\n".join(context)
            return result

        return ""

    except Exception as e:
        print(f"[ENTITY SEARCH ERROR] {e}")
        return ""

# ─── 7. SQL VALIDATOR ─────────────────────────────────────────────────────────
def validate_sql(sql: str) -> tuple:
    sql_upper = sql.upper().strip()

    dangerous = ["UPDATE", "DELETE", "INSERT", "DROP", "ALTER",
                 "TRUNCATE", "CREATE", "GRANT", "REVOKE"]
    for op in dangerous:
        if re.search(r'\b' + op + r'\b', sql_upper):
            return False, f"Operasi {op} tidak diizinkan."

    if re.search(r'SELECT\s+\*', sql_upper):
        return False, "Query SELECT * tidak diizinkan."

    if not re.search(r'\bSELECT\b', sql_upper):
        return False, "Hanya query SELECT yang diizinkan."

    if "LIMIT" not in sql_upper:
        sql = sql.rstrip(";") + " LIMIT 50"

    return True, sql

# ─── 8. EXECUTE QUERY ─────────────────────────────────────────────────────────
def execute_query(sql: str) -> tuple:
    valid, result = validate_sql(sql)
    if not valid:
        return [], []

    try:
        conn = psycopg2.connect(DATABASE_URL)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(result)
            rows    = cur.fetchall()
            columns = [desc[0] for desc in cur.description] if cur.description else []
            data    = [dict(row) for row in rows]
        conn.close()
        return data, columns
    except Exception as e:
        print(f"[QUERY ERROR] {e}")
        return [], []

# ─── 9. FORMAT DATA TO TELEGRAM TEXT ──────────────────────────────────────────
def format_data_for_tg(data: list, columns: list, original_question: str, narrative_hint: str) -> str:
    if not data:
        return "Tidak ditemukan data yang sesuai. 🔍"

    row_count = len(data)

    if row_count <= 5 and len(columns) <= 6:
        clean_data = []
        for row in data:
            clean_row = {}
            for k, v in row.items():
                clean_row[k] = v.isoformat() if hasattr(v, 'isoformat') else v
            clean_data.append(clean_row)

        try:
            narrative = call_ai([
                {
                    "role": "system",
                    "content": (
                        "Kamu adalah asisten laporan bisnis via Telegram. "
                        "Jawab dalam Bahasa Indonesia yang profesional dan natural. "
                        "Gunakan format Telegram: *bold* untuk poin penting, • untuk list, emoji relevan. "
                        "Jangan gunakan tabel HTML. Jika ada nilai uang, format sebagai Rupiah (Rp 1.250.000.000)."
                    )
                },
                {
                    "role": "user",
                    "content": (
                        f'Pertanyaan user: "{original_question}"\n'
                        f"Hint narasi: {narrative_hint}\n"
                        f"Data hasil query: {json.dumps(clean_data, default=str, ensure_ascii=False)}\n\n"
                        "Buatkan narasi singkat dalam Bahasa Indonesia yang menjawab pertanyaan tersebut. "
                        "Maksimal 5 kalimat atau poin."
                    )
                }
            ], max_tokens=600)
            return narrative
        except Exception as e:
            print(f"[NARRATIVE ERROR] {e}")

    lines = [f"📊 *Ditemukan {row_count} data*\n"]
    display_data = data[:7]

    for row in display_data:
        parts = []
        for col in columns[:4]:
            val = row.get(col)
            if val is None:
                continue
            if hasattr(val, 'isoformat'):
                val = val.strftime('%d/%m/%Y') if hasattr(val, 'strftime') else val.isoformat()
            parts.append(f"{col}: {val}")
        lines.append("• " + " | ".join(parts))

    if row_count > 7:
        lines.append(f"\n_(Menampilkan 7 dari {row_count} data, tanya lebih spesifik untuk detail)_")

    return "\n".join(lines)

# ─── 10. MEMORY PER USER ──────────────────────────────────────────────────────
MAX_HISTORY    = 10
user_histories: dict = {}

def get_history(user_id: int) -> list:
    return user_histories.get(user_id, [])

def add_history(user_id: int, question: str, answer: str):
    history = user_histories.get(user_id, [])
    history.append({"role": "user",      "content": question})
    history.append({"role": "assistant", "content": answer})
    if len(history) > MAX_HISTORY * 2:
        history = history[-(MAX_HISTORY * 2):]
    user_histories[user_id] = history

def clear_history(user_id: int):
    user_histories.pop(user_id, None)

# ─── 10b. FITUR #LAPORAN ──────────────────────────────────────────────────────

LAPORAN_SYSTEM_PROMPT = (
    "Kamu adalah parser laporan harian maintenance kilang minyak.\n"
    "Tugasmu mengekstrak data dari teks laporan narasi ke dalam format JSON terstruktur.\n\n"
    "DISIPLIN YANG VALID: Electrical, Instrument, Rotating, Stationary, Alat Berat\n\n"
    "KATEGORI YANG VALID:\n"
    "- Corrective Maintenance\n"
    "- Preventive Maintenance\n"
    "- Plant Patrol\n"
    "- Progress\n"
    "- Challenge Session\n"
    "- Support\n\n"
    "STATUS YANG VALID: Done, In Progress, Waiting Material, Pending, -\n"
    "MAPPING STATUS (terapkan tepat seperti ini):\n"
    "  (done), DONE, Done, selesai → Done\n"
    "  (ip), (in progress), In Progress, in progress, sedang dikerjakan → In Progress\n"
    "  waiting material, wm → Waiting Material\n"
    "  pending → Pending\n"
    "  Jika tidak ada keterangan status → -\n\n"
    "DIREKSI (area kerja) YANG VALID: MA5, MA6, MA7, Workshop\n"
    "Normalisasi: 'Maintenance Area 7' / 'Area 7' / 'MA 7' / 'Bagian 7' → 'MA7'\n"
    "             'Maintenance Area 5' / 'Area 5' / 'MA 5' / 'Bagian 5' → 'MA5'\n"
    "             'Maintenance Area 6' / 'Area 6' / 'MA 6' / 'Bagian 6' → 'MA6'\n"
    "             'Workshop' → 'Workshop'\n"
    "Jika tidak ada informasi direksi, gunakan string kosong.\n\n"
    "TAG NUMBER: Kode identifikasi equipment/alat yang biasanya ada di awal deskripsi item,\n"
    "dipisah dengan titik dua (:) atau spasi. Contoh: 101-P-105, 104-P-107, 101A514, 105-FV-020.\n"
    "Format umum: [area]-[tipe]-[nomor] atau [area][kode][nomor].\n"
    "Jika tidak ada tag number, gunakan string kosong.\n\n"
    "ATURAN EKSTRAKSI:\n"
    "1. Satu item pekerjaan = satu entri JSON\n"
    "2. Deteksi tanggal dari teks laporan (format DD/MM/YYYY, DD Bulan YYYY, dsb)\n"
    "3. Deteksi disiplin dari header laporan\n"
    "4. Deteksi direksi dari header laporan, normalisasi ke MA5/MA6/MA7/Workshop\n"
    "5. Petakan setiap item ke kategori yang sesuai\n"
    "6. Ekstrak status menggunakan MAPPING STATUS di atas\n"
    "7. Ekstrak tag number dari awal deskripsi item jika ada\n"
    "8. Deskripsi diisi tanpa tag number (tag number sudah dipisah di field tag_number)\n"
    "9. Catatan: info tambahan yang relevan (target tanggal, detail teknis, dll)\n\n"
    "ATURAN MULTI-TAG (penting):\n"
    "Jika satu baris menyebut beberapa tag sekaligus, buat SATU entri per tag.\n"
    "Contoh: 'Perbaikan fireproofing: 105-P-506, 105-P-508, 105-P-507 (in progress)'\n"
    "→ 3 entri terpisah, masing-masing dengan tag berbeda, deskripsi & status sama.\n\n"
    "ATURAN ABAIKAN BARIS BERIKUT (jangan buat entri JSON):\n"
    "- Baris template/placeholder, contoh: 'Tag Number/Equipment/Func. Loc: Aktifitas (status)', '...', '..'\n"
    "- Baris sub-header equipment, contoh: '* Equipment : Transmitter', '* Equipment : Junction Box'\n"
    "- Baris kosong atau hanya berisi tanda baca\n\n"
    "RESPONSE FORMAT — kembalikan HANYA array JSON, tanpa teks lain:\n"
    '[\n  {\n    "tanggal_laporan": "2026-05-26",\n    "disiplin": "Instrument",\n'
    '    "direksi": "MA7",\n    "kategori": "Plant Patrol",\n    "tag_number": "105-FV-020",\n'
    '    "deskripsi": "Plant Patrol control valve",\n'
    '    "status_pekerjaan": "Done",\n    "catatan": ""\n  }\n]\n\n'
    "PENTING: Kembalikan HANYA array JSON yang valid. Jangan tambahkan penjelasan apapun."
)

# ── Pola untuk filter entri sampah post-parse ──────────────────────────────────
PLACEHOLDER_PATTERNS = [
    r'^tag\s*number',
    r'^func.*loc',
    r'^\.*$',
    r'^aktifitas',
    r'^\s*\.\.\.*\s*$',
]

def is_junk_entry(item: dict) -> bool:
    """Return True kalau entri ini template/placeholder/sampah."""
    deskripsi  = item.get("deskripsi",  "").strip().lower()
    tag_number = item.get("tag_number", "").strip().lower()
    combined   = f"{tag_number} {deskripsi}".strip()
    for pat in PLACEHOLDER_PATTERNS:
        if re.search(pat, combined, re.IGNORECASE):
            return True
    # Entri tanpa deskripsi DAN tanpa tag → sampah
    if not deskripsi and not tag_number:
        return True
    return False


def extract_laporan_header(raw_text: str) -> dict:
    """
    Ekstrak header laporan (tanggal, semua disiplin, direksi) secara regex
    tanpa perlu AI — cepat dan hemat token.
    disiplin sekarang berupa list (bisa lebih dari satu).
    """
    header = {"tanggal": "", "disiplin": [], "direksi": ""}

    # ── Tanggal ────────────────────────────────────────────────────────────────
    BULAN_MAP = {
        "januari": "01", "februari": "02", "maret": "03", "april": "04",
        "mei": "05", "juni": "06", "juli": "07", "agustus": "08",
        "september": "09", "oktober": "10", "november": "11", "desember": "12"
    }
    m = re.search(r'\b(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})\b', raw_text)
    if m:
        header["tanggal"] = f"{m.group(3)}-{m.group(2).zfill(2)}-{m.group(1).zfill(2)}"
    else:
        m = re.search(
            r'\b(\d{1,2})\s+(' + '|'.join(BULAN_MAP.keys()) + r')\s+(\d{4})\b',
            raw_text, re.IGNORECASE
        )
        if m:
            bln = BULAN_MAP[m.group(2).lower()]
            header["tanggal"] = f"{m.group(3)}-{bln}-{m.group(1).zfill(2)}"

    # ── Disiplin — kumpulkan semua yang ada ───────────────────────────────────
    DISIPLIN_LIST = ["Electrical", "Instrument", "Rotating", "Stationary", "Alat Berat"]
    for d in DISIPLIN_LIST:
        if re.search(r'\b' + d + r'\b', raw_text, re.IGNORECASE):
            header["disiplin"].append(d)
    # Tangkap "INSTRUMENTASI" → "Instrument"
    if re.search(r'\bINSTRUMENTASI\b', raw_text, re.IGNORECASE):
        if "Instrument" not in header["disiplin"]:
            header["disiplin"].append("Instrument")

    # ── Direksi / Area ─────────────────────────────────────────────────────────
    DIREKSI_MAP = {
        r'\bMA\s*7\b': "MA7", r'\bMA\s*6\b': "MA6", r'\bMA\s*5\b': "MA5",
        r'[Mm]aintenance\s+[Aa]rea\s*7': "MA7",
        r'[Mm]aintenance\s+[Aa]rea\s*6': "MA6",
        r'[Mm]aintenance\s+[Aa]rea\s*5': "MA5",
        r'\bWorkshop\b': "Workshop",
    }
    for pattern, val in DIREKSI_MAP.items():
        if re.search(pattern, raw_text):
            header["direksi"] = val
            break

    return header


def split_laporan_to_chunks(raw_text: str) -> list:
    """
    Potong teks laporan secara hierarkis: disiplin → kategori.
    Setiap chunk membawa header disiplinnya sendiri sehingga AI
    dapat men-tag disiplin dengan benar.
    Chunk yang terlalu besar (>MAX_ITEMS_PER_CHUNK baris item)
    dipecah otomatis agar tidak melebihi max_tokens output.
    Mengembalikan list of (header_text, chunk_text).
    """
    DISIPLIN_PATTERNS = {
        "Rotating":   r'^ROTATING\s*$',
        "Stationary": r'^STATIONARY\s*$',
        "Electrical": r'^ELECTRICAL\s*$',
        "Instrument": r'^INSTRUMENTASI\s*$|^INSTRUMENT\s*$',
        "Alat Berat": r'^ALAT\s*BERAT\s*$',
    }
    CATEGORY_KEYWORDS = [
        "Plant Patrol", "Preventive Maintenance", "Corrective Maintenance",
        "Progress", "Challenge Session", "Project", "Support",
    ]
    MAX_ITEMS_PER_CHUNK = 25

    lines = raw_text.splitlines()

    # ── Pecah per disiplin ─────────────────────────────────────────────────────
    sections = []          # list of (disiplin_str, lines_list)
    current_disiplin = ""
    current_lines    = []

    for line in lines:
        stripped = line.strip()
        matched_disiplin = None
        for d, pat in DISIPLIN_PATTERNS.items():
            if re.match(pat, stripped, re.IGNORECASE):
                matched_disiplin = d
                break

        if matched_disiplin:
            if current_lines:
                sections.append((current_disiplin, current_lines))
            current_disiplin = matched_disiplin
            current_lines    = [line]
        else:
            current_lines.append(line)

    if current_lines:
        sections.append((current_disiplin, current_lines))

    # ── Fallback: tidak ada penanda disiplin → logika lama (split per kategori) ─
    has_discipline = any(d for d, _ in sections)
    if not has_discipline:
        cat_pattern = (
            r'(?:(?<=\n)|(?:^))(?='
            + '|'.join(re.escape(k) for k in CATEGORY_KEYWORDS)
            + r')'
        )
        parts = re.split(cat_pattern, raw_text, flags=re.IGNORECASE | re.MULTILINE)
        header_lines, chunks_old = [], []
        for part in parts:
            part = part.strip()
            if not part:
                continue
            if any(part.lower().startswith(k.lower()) for k in CATEGORY_KEYWORDS):
                chunks_old.append(part)
            else:
                header_lines.append(part)
        header_text = "\n".join(header_lines)
        if not chunks_old:
            return [(header_text, raw_text)]
        return [(header_text, c) for c in chunks_old]

    # ── Pisahkan header global (baris sebelum disiplin pertama) ───────────────
    global_header = ""
    if sections and not sections[0][0]:
        global_header = "\n".join(sections[0][1]).strip()
        sections = sections[1:]

    # ── Per section disiplin, pecah lagi per kategori ─────────────────────────
    result_chunks = []

    for disiplin, disc_lines in sections:
        disc_text = "\n".join(disc_lines)

        cat_pattern = (
            r'(?:(?<=\n)|(?:^))(?='
            + '|'.join(re.escape(k) for k in CATEGORY_KEYWORDS)
            + r')'
        )
        parts = re.split(cat_pattern, disc_text, flags=re.IGNORECASE | re.MULTILINE)

        disc_header_parts, cat_chunks = [], []
        for part in parts:
            part = part.strip()
            if not part:
                continue
            if any(part.lower().startswith(k.lower()) for k in CATEGORY_KEYWORDS):
                cat_chunks.append(part)
            else:
                disc_header_parts.append(part)

        disc_header    = "\n".join(disc_header_parts).strip()
        combined_header = "\n".join(filter(None, [global_header, disc_header]))

        if not cat_chunks:
            result_chunks.append((combined_header, disc_text))
            continue

        for cat_chunk in cat_chunks:
            # Pecah chunk besar menjadi beberapa batch
            item_lines = [
                l for l in cat_chunk.splitlines()
                if re.match(r'^\s*[\d\*\-•]', l)
            ]
            if len(item_lines) > MAX_ITEMS_PER_CHUNK:
                cat_header_line = cat_chunk.splitlines()[0]
                for i in range(0, len(item_lines), MAX_ITEMS_PER_CHUNK):
                    batch     = item_lines[i:i + MAX_ITEMS_PER_CHUNK]
                    sub_chunk = cat_header_line + "\n" + "\n".join(batch)
                    result_chunks.append((combined_header, sub_chunk))
            else:
                result_chunks.append((combined_header, cat_chunk))

    return result_chunks


def parse_chunk_with_ai(header_text: str, chunk_text: str) -> list:
    """Parse satu chunk (satu kategori/disiplin) dengan AI."""
    combined = f"{header_text}\n\n{chunk_text}" if header_text else chunk_text
    try:
        response = call_ai([
            {"role": "system", "content": LAPORAN_SYSTEM_PROMPT},
            {"role": "user",   "content": f"Parse laporan berikut:\n\n{combined}"}
        ], max_tokens=3000)

        json_match = re.search(r'\[[\s\S]*\]', response)
        if not json_match:
            print(f"[PARSE CHUNK] Tidak ada JSON: {response[:200]}")
            return []

        parsed = json.loads(json_match.group())
        if not isinstance(parsed, list):
            return []

        # Filter entri sampah/placeholder
        clean = [item for item in parsed if not is_junk_entry(item)]
        filtered_count = len(parsed) - len(clean)
        if filtered_count:
            print(f"[PARSE CHUNK] Dibuang {filtered_count} entri sampah/placeholder")

        return clean

    except Exception as e:
        print(f"[PARSE CHUNK ERROR] {e}")
        return []


def parse_laporan_with_ai(raw_text: str) -> list:
    """
    Parse laporan dengan chunking hierarkis per disiplin → kategori.
    Setiap chunk diparse terpisah lalu digabungkan.
    """
    chunks = split_laporan_to_chunks(raw_text)
    print(f"[PARSE LAPORAN] Total chunks: {len(chunks)}")

    all_items = []
    for i, (header_text, chunk_text) in enumerate(chunks):
        print(f"[PARSE LAPORAN] Memproses chunk {i+1}/{len(chunks)}: "
              f"{chunk_text[:60].strip()!r}...")
        items = parse_chunk_with_ai(header_text, chunk_text)
        print(f"[PARSE LAPORAN] Chunk {i+1} → {len(items)} item")
        all_items.extend(items)

    # Deduplikasi: tanggal + disiplin + tag_number + deskripsi
    seen = set()
    unique_items = []
    for item in all_items:
        key = (
            item.get("tanggal_laporan", ""),
            item.get("disiplin", ""),
            item.get("tag_number", "").strip().lower(),
            item.get("deskripsi", "").strip().lower(),
        )
        if key not in seen:
            seen.add(key)
            unique_items.append(item)

    print(f"[PARSE LAPORAN] Total item unik: {len(unique_items)}")
    return unique_items


def insert_daily_report(items: list, pengirim: str, raw_text: str) -> tuple:
    """
    Simpan items ke daily_report.
    Return: (success_count, error_msg | None, dup_warnings_list)
    """
    if not items:
        return 0, "Tidak ada item yang bisa diparse", []

    # Fallback direksi dari raw_text kalau item tidak punya
    fallback_direksi = ""
    DIREKSI_MAP_FB = {
        r'\bMA\s*7\b': "MA7", r'\bMA\s*6\b': "MA6", r'\bMA\s*5\b': "MA5",
        r'[Mm]aintenance\s+[Aa]rea\s*7': "MA7",
        r'[Mm]aintenance\s+[Aa]rea\s*6': "MA6",
        r'[Mm]aintenance\s+[Aa]rea\s*5': "MA5",
        r'\bWorkshop\b': "Workshop",
    }
    for pat, val in DIREKSI_MAP_FB.items():
        if re.search(pat, raw_text):
            fallback_direksi = val
            break

    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur  = conn.cursor()
        success      = 0
        skipped      = 0
        dup_warnings = []
        saved_items  = []

        for item in items:
            if not item.get("tanggal_laporan") or not item.get("disiplin") or not item.get("deskripsi"):
                skipped += 1
                continue

            # Fallback direksi jika kosong
            direksi = item.get("direksi", "") or fallback_direksi

            # Cek duplikat di DB
            cur.execute("""
                SELECT id_report FROM daily_report
                WHERE tanggal_laporan = %s
                  AND disiplin        = %s
                  AND tag_number      = %s
                  AND deskripsi       = %s
                LIMIT 1
            """, (
                item.get("tanggal_laporan"),
                item.get("disiplin", "-"),
                item.get("tag_number", ""),
                item.get("deskripsi", "-"),
            ))
            if cur.fetchone():
                label = item.get("tag_number") or item.get("deskripsi", "?")[:30]
                dup_warnings.append(label)
                continue

            cur.execute("""
                INSERT INTO daily_report
                    (tanggal_laporan, disiplin, direksi, kategori, tag_number, deskripsi,
                     status_pekerjaan, catatan, pengirim_wa, raw_text)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                item.get("tanggal_laporan"),
                item.get("disiplin",        "-"),
                direksi,
                item.get("kategori",        "-"),
                item.get("tag_number",      ""),
                item.get("deskripsi",       "-"),
                item.get("status_pekerjaan","-"),
                item.get("catatan",         ""),
                pengirim,
                raw_text,
            ))
            saved_items.append(item)
            success += 1

        conn.commit()
        conn.close()
        if skipped:
            print(f"[INSERT LAPORAN] {skipped} item dilewati (field wajib kosong)")
        return success, None, dup_warnings, saved_items

    except Exception as e:
        print(f"[INSERT LAPORAN ERROR] {e}")
        return 0, str(e), []


def process_laporan(raw_text: str, pengirim: str) -> str:
    print(f"[LAPORAN] Memproses dari {pengirim}, panjang: {len(raw_text)} karakter")

    header   = extract_laporan_header(raw_text)
    chunks   = split_laporan_to_chunks(raw_text)
    n_chunks = len(chunks)

    items = parse_laporan_with_ai(raw_text)
    if not items:
        return (
            "⚠️ *Gagal memparse laporan.*\n\n"
            "Pastikan format laporan sudah benar:\n"
            "• Ada tanggal (contoh: 03/06/2026 atau 3 Juni 2026)\n"
            "• Ada disiplin (Electrical/Instrument/Rotating/Stationary/Alat Berat)\n"
            "• Ada kategori pekerjaan (Plant Patrol / Preventive / Corrective / Project)\n"
            "• Ada daftar item pekerjaan\n\n"
            "Coba kirim ulang dengan format yang lebih jelas."
        )

    success_count, error, dup_warnings, saved_items = insert_daily_report(items, pengirim, raw_text)
    if error:
        return f"⚠️ *Gagal menyimpan laporan:* {error}"
    if success_count == 0 and not dup_warnings:
        return "⚠️ *Tidak ada data yang berhasil disimpan.* Periksa format laporan."

    # Ringkasan per kategori
    summary = {}
    for item in saved_items:
        key = f"{item.get('disiplin', '-')} — {item.get('kategori', '-')}"
        summary[key] = summary.get(key, 0) + 1
    summary_lines = "\n".join([f"  • {k}: {v} item" for k, v in summary.items()])

    disiplin_str = ", ".join(header["disiplin"]) if header["disiplin"] else "-"
    tgl_info     = f"📅 *Tanggal:* {header['tanggal']}\n"              if header["tanggal"]  else ""
    area_info    = f"🏭 *Area:* {header['direksi']} — {disiplin_str}\n" if header["direksi"]  else ""
    chunk_info   = f"🔀 *Diproses dalam:* {n_chunks} bagian\n"          if n_chunks > 1       else ""

    # Info duplikat
    dup_info = ""
    if dup_warnings:
        dup_list = ", ".join(dup_warnings[:5])
        lebih    = f" (+{len(dup_warnings)-5} lainnya)" if len(dup_warnings) > 5 else ""
        dup_info = f"\n⚠️ *Duplikat dilewati ({len(dup_warnings)} item):* _{dup_list}{lebih}_\n"

    return (
        f"✅ *Laporan berhasil disimpan!*\n\n"
        f"{tgl_info}{area_info}{chunk_info}\n"
        f"📋 *Total:* {success_count} kegiatan tercatat\n"
        f"{dup_info}\n"
        f"*Rincian per kategori:*\n{summary_lines}\n\n"
        f"_Data tersimpan di database dan bisa ditanyakan kapan saja._"
    )

# ─── 11. CORE FUNCTION ────────────────────────────────────────────────────────
def run_query(question: str, user_id: int) -> str:
    dynamic_context = smart_entity_search(question)
    system_prompt   = BASE_SYSTEM_PROMPT + dynamic_context

    history  = get_history(user_id)
    messages = [{"role": "system", "content": system_prompt}]
    messages += history[-MAX_HISTORY * 2:]
    messages.append({"role": "user", "content": question})

    try:
        raw = call_ai(messages, max_tokens=1500)

        json_match = re.search(r'\{[\s\S]*\}', raw)
        if not json_match:
            answer = raw
            add_history(user_id, question, answer)
            return answer

        parsed        = json.loads(json_match.group())
        response_type = parsed.get("type", "narrative")

        if response_type == "clarification":
            answer = parsed.get("clarification_question", parsed.get("message", ""))
            add_history(user_id, question, answer)
            return answer

        if response_type == "error":
            answer = parsed.get("message", "Maaf, terjadi kesalahan dalam memproses pertanyaan Anda.")
            add_history(user_id, question, answer)
            return answer

        if response_type == "narrative":
            answer = parsed.get("message", raw)
            add_history(user_id, question, answer)
            return answer

        if response_type == "query" and parsed.get("sql"):
            sql = parsed["sql"]
            valid, val_result = validate_sql(sql)
            if not valid:
                answer = f"⚠️ Maaf, query tidak valid: {val_result}"
                add_history(user_id, question, answer)
                return answer

            data, columns = execute_query(val_result)

            if not data:
                answer = "🔍 Tidak ditemukan data yang sesuai dengan pertanyaan Anda."
            else:
                answer = format_data_for_tg(
                    data, columns, question,
                    parsed.get("narrative_hint", "")
                )

            explanation = parsed.get("explanation", "")
            if explanation and len(data) > 0:
                answer = f"_{explanation}_\n\n" + answer

            add_history(user_id, question, answer)
            return answer

        answer = parsed.get("message", raw)
        add_history(user_id, question, answer)
        return answer

    except json.JSONDecodeError:
        answer = raw
        add_history(user_id, question, answer)
        return answer
    except Exception as e:
        print(f"[RUN_QUERY ERROR] {e}")
        return "⚠️ Maaf, terjadi kesalahan sistem. Silakan coba beberapa saat lagi."

# ─── 12. TELEGRAM HANDLERS ────────────────────────────────────────────────────
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id  = update.effective_user.id
    is_group = update.message.chat.type in ("group", "supergroup")
    clear_history(user_id)

    if is_group:
        bot_username = (await context.bot.get_me()).username
        await update.message.reply_text(
            f"👋 *Halo semua!*\n\n"
            f"Saya siap membantu analisis data *manajemen kontrak kilang minyak* 🏭\n\n"
            f"💡 Cara pakai di grup — mention saya:\n"
            f"`@{bot_username} berapa kontrak aktif di MA5?`\n\n"
            f"Kirim *#laporan* diikuti isi laporan untuk menyimpan laporan harian.\n\n"
            f"Ketik /reset untuk memulai sesi baru.",
            parse_mode='Markdown'
        )
    else:
        await update.message.reply_text(
            "👋 *Halo!* Selamat datang di *Bot Manajemen Kontrak Kilang* 🏭\n\n"
            "Saya dapat membantu Anda:\n"
            "• 📋 Cek status & detail kontrak\n"
            "• 💰 Informasi tagihan & pembayaran\n"
            "• 🏢 Data vendor & performanya\n"
            "• 📊 Progress pekerjaan & milestone\n"
            "• 📄 Status dokumen & amandemen\n"
            "• 📝 Simpan laporan harian dengan *#laporan*\n\n"
            "💡 *Tips:* Tanyakan analisis atau data spesifik, bukan 'tampilkan semua'.\n\n"
            "Ketik /reset untuk memulai percakapan baru.",
            parse_mode='Markdown'
        )

async def cmd_reset(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    clear_history(user_id)
    await update.message.reply_text(
        "🔄 *Percakapan direset.* Memori sesi sebelumnya dihapus.",
        parse_mode='Markdown'
    )

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🤖 *Panduan Bot Kontrak Kilang*\n\n"
        "*Contoh pertanyaan:*\n"
        "• Kontrak apa saja yang aktif di MA5?\n"
        "• Berapa nilai kontrak PT ABC?\n"
        "• Tagihan mana yang belum dibayar?\n"
        "• Progress kontrak nomor KOM-001?\n"
        "• Vendor mana yang punya kontrak Lumpsum?\n"
        "• Amandemen kontrak bulan ini?\n"
        "• Laporan Rotating hari ini?\n\n"
        "*Kirim Laporan Harian:*\n"
        "#laporan [isi laporan]\n"
        "Contoh:\n"
        "`#laporan Pekerjaan Rotating MA7 26 Mei 2026`\n"
        "`Corrective Maintenance`\n"
        "`1. 101-P-103: Perbaikan koneksi SAF (done)`\n\n"
        "*Perintah:*\n"
        "/start — Salam pembuka\n"
        "/reset — Hapus memori percakapan\n"
        "/help  — Tampilkan panduan ini",
        parse_mode='Markdown'
    )

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return

    user_id  = update.effective_user.id
    text     = update.message.text.strip()
    is_group = update.message.chat.type in ("group", "supergroup")
    pengirim = str(update.effective_user.id)

    # ── Di grup: hanya proses jika di-mention ─────────────────────────────────
    if is_group:
        bot_username = (await context.bot.get_me()).username
        mention      = f"@{bot_username}"
        if mention not in text:
            # Cek #laporan di grup tanpa perlu mention
            if not any(text.lower().startswith(t) for t in ["#laporan", "#report", "#lpr"]):
                return
            question = text
        else:
            question = text.replace(mention, "").strip()
            if not question:
                await update.message.reply_text(
                    f"👋 Silakan ajukan pertanyaan setelah mention saya.\n"
                    f"Contoh: *{mention} berapa kontrak aktif di MA5?*",
                    parse_mode='Markdown'
                )
                return
    else:
        question = text

    # ── Deteksi #laporan ───────────────────────────────────────────────────────
    LAPORAN_TRIGGERS = ["#laporan", "#report", "#lpr"]
    matched_laporan  = None
    for trigger in LAPORAN_TRIGGERS:
        if question.lower().startswith(trigger):
            matched_laporan = trigger
            break

    if matched_laporan:
        laporan_text = question[len(matched_laporan):].strip()
        if not laporan_text:
            await update.message.reply_text(
                "📋 *Format pengiriman laporan:*\n\n"
                "*#laporan* [isi laporan]\n\n"
                "Contoh:\n"
                "`#laporan Pekerjaan Rotating MA7 26 Mei 2026`\n"
                "`Corrective Maintenance`\n"
                "`1. 101-P-103: Perbaikan koneksi SAF (done)`",
                parse_mode='Markdown'
            )
            return

        await context.bot.send_chat_action(
            chat_id=update.effective_chat.id,
            action=ChatAction.TYPING
        )
        loop   = asyncio.get_event_loop()
        answer = await loop.run_in_executor(None, process_laporan, laporan_text, pengirim)

        try:
            await update.message.reply_text(answer, parse_mode='Markdown')
        except Exception:
            await update.message.reply_text(answer)
        return

    # ── Kirim "typing..." indicator ───────────────────────────────────────────
    await context.bot.send_chat_action(
        chat_id=update.effective_chat.id,
        action=ChatAction.TYPING
    )

    # ── Proses query di executor ───────────────────────────────────────────────
    loop   = asyncio.get_event_loop()
    answer = await loop.run_in_executor(None, run_query, question, user_id)

    if len(answer) > 4096:
        answer = answer[:4000] + "\n\n_...(pesan terpotong, tanya lebih spesifik)_"

    try:
        await update.message.reply_text(answer, parse_mode='Markdown')
    except Exception:
        await update.message.reply_text(answer)

# ─── 13. RUN ──────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if not TELEGRAM_BOT_TOKEN:
        print("❌ Error: TELEGRAM_BOT_TOKEN belum diset!")
    else:
        print("🚀 Bot Telegram Kontrak Kilang berjalan...")
        app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
        app.add_handler(CommandHandler("start", cmd_start))
        app.add_handler(CommandHandler("reset", cmd_reset))
        app.add_handler(CommandHandler("help",  cmd_help))
        app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_message))
        app.run_polling()