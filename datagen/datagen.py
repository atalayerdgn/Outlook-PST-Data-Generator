"""Data Generator

extract/metadata altındaki her hesap klasörünün (ör: backup, test@cinergroup.com.tr)
içindeki en güncel emails_*.csv dosyalarını bularak tek birleştirilmiş bir dataset
oluşturur ve opsiyonel olarak sentetik (üretim) kayıtlar ekler.

Örnek Kullanım:
  python datagen.py --metadata-dir ../extract/metadata --out-dir ./output
  python datagen.py -m ../extract/metadata -o ./output --synthesize 200

Çıktılar:
  output/
	merged_emails_<timestamp>.csv
	stats_<timestamp>.json
	(opsiyonel) synthetic_info_<timestamp>.json

Notlar:
  - emails_*.csv kolonları: id,folder,subject,sender_name,sender_email,delivery_time,size,attachments_count
  - Birleştirilmiş dosyaya eklenen ekstra kolonlar: account, source_file, synthetic_flag
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional
import random
import hashlib

try:
	from faker import Faker  # type: ignore
	_FAKER_AVAILABLE = True
except Exception:  # pragma: no cover
	_FAKER_AVAILABLE = False
	Faker = None  # type: ignore


LOGGER = logging.getLogger("datagen")


def setup_logging(verbose: bool):
	level = logging.DEBUG if verbose else logging.INFO
	logging.basicConfig(level=level, format='%(asctime)s %(levelname)s %(message)s')


@dataclass
class EmailRecord:
	id: str
	folder: str
	subject: str
	sender_name: str
	sender_email: str
	delivery_time: str
	size: str
	attachments_count: str
	# Ek alanlar
	account: str
	source_file: str
	synthetic_flag: int = 0

	def to_row(self) -> Dict[str, str]:
		return {
			'id': self.id,
			'folder': self.folder,
			'subject': self.subject,
			'sender_name': self.sender_name,
			'sender_email': self.sender_email,
			'delivery_time': self.delivery_time,
			'size': self.size,
			'attachments_count': self.attachments_count,
			'account': self.account,
			'source_file': self.source_file,
			'synthetic_flag': str(self.synthetic_flag),
		}


EMAIL_CSV_COLUMNS = [
	'id', 'folder', 'subject', 'sender_name', 'sender_email', 'delivery_time',
	'size', 'attachments_count'
]

OUTPUT_COLUMNS = EMAIL_CSV_COLUMNS + ['account', 'source_file', 'synthetic_flag']

# Varsayılan klasör havuzu (karışık üretim için)
DEFAULT_FOLDER_POOL = [
	'Outlook veri dosyasının en üstü/Gelen Kutusu',
	'Outlook veri dosyasının en üstü/Gönderilmiş Öğeler',
	'Outlook veri dosyasının en üstü/Taslaklar',
	'Outlook veri dosyasının en üstü/Silinen Öğeler',
	'Outlook veri dosyasının en üstü/Arşiv',
	'Outlook veri dosyasının en üstü/Gelen Kutusu/Projeler/Proje A',
	'Outlook veri dosyasının en üstü/Gelen Kutusu/Projeler/Proje B',
	'Outlook veri dosyasının en üstü/Gelen Kutusu/Müşteriler/Önemli',
]

# Ağırlıklar (uzun vadede dağılım kontrolü)
DEFAULT_FOLDER_WEIGHTS = [0.42, 0.18, 0.07, 0.06, 0.05, 0.08, 0.08, 0.06]


def find_latest_email_csv(account_dir: Path) -> Optional[Path]:
	candidates = sorted(account_dir.glob('emails_*.csv'))
	if not candidates:
		return None
	# Dosya adındaki timestamp'e göre sıralamak için parse etmeyi dener
	def ts(p: Path):
		try:
			# emails_YYYYMMDD_HHMMSS.csv
			stem = p.stem  # emails_YYYYMMDD_HHMMSS
			part = stem.split('_', 1)[1]
			dt = datetime.strptime(part, '%Y%m%d_%H%M%S')
			return dt
		except Exception:
			return datetime.fromtimestamp(p.stat().st_mtime)
	candidates.sort(key=ts, reverse=True)
	return candidates[0]


def load_emails(csv_path: Path, account: str) -> List[EmailRecord]:
	records: List[EmailRecord] = []
	try:
		with csv_path.open('r', encoding='utf-8') as f:
			reader = csv.DictReader(f)
			missing = [c for c in EMAIL_CSV_COLUMNS if c not in reader.fieldnames]
			if missing:
				LOGGER.warning("Eksik kolon(lar) %s (%s)", missing, csv_path)
			for row in reader:
				try:
					rec = EmailRecord(
						id=row.get('id', ''),
						folder=row.get('folder', ''),
						subject=row.get('subject', ''),
						sender_name=row.get('sender_name', ''),
						sender_email=row.get('sender_email', ''),
						delivery_time=row.get('delivery_time', ''),
						size=str(row.get('size', '0')),
						attachments_count=str(row.get('attachments_count', '0')),
						account=account,
						source_file=str(csv_path),
					)
					records.append(rec)
				except Exception as e:  # pragma: no cover
					LOGGER.debug("Satır atlandı (%s): %s", csv_path, e)
	except FileNotFoundError:
		LOGGER.error("Dosya bulunamadı: %s", csv_path)
	return records


def generate_synthetic(base_records: List[EmailRecord], count: int, locale: str = 'tr_TR') -> List[EmailRecord]:
	if count <= 0 or not base_records:
		return []
	faker = Faker(locale) if _FAKER_AVAILABLE else None
	synthetic: List[EmailRecord] = []
	for _ in range(count):
		template = random.choice(base_records)
		# Yeni subject ve sender üret
		if faker:
			new_subject = faker.sentence(nb_words=random.randint(3, 9)).rstrip('.')
			sender_name = faker.name()
			sender_email = faker.email()
		else:  # Basit degrade fallback
			new_subject = template.subject + f" #{random.randint(1,999)}"
			sender_name = template.sender_name or "Sender"
			sender_email = template.sender_email or f"user{random.randint(1,999)}@example.com"
		# Random tarih - son 365 gün
		dt = datetime.now() - timedelta(days=random.randint(0, 365), hours=random.randint(0, 23), minutes=random.randint(0, 59))
		delivery_time = dt.strftime('%Y-%m-%d %H:%M:%S')
		# ID üretimi
		raw_id = f"{new_subject}{sender_email}{delivery_time}{random.random()}".encode()
		rec_id = hashlib.md5(raw_id).hexdigest()[:16]
		synth = EmailRecord(
			id=rec_id,
			folder=template.folder,
			subject=new_subject,
			sender_name=sender_name,
			sender_email=sender_email,
			delivery_time=delivery_time,
			size=template.size,
			attachments_count=template.attachments_count,
			account=template.account + "_synthetic",
			source_file='synthetic',
			synthetic_flag=1,
		)
		synthetic.append(synth)
	return synthetic


def write_merged(records: List[EmailRecord], out_dir: Path) -> Path:
	out_dir.mkdir(parents=True, exist_ok=True)
	ts = datetime.now().strftime('%Y%m%d_%H%M%S')
	out_csv = out_dir / f"merged_emails_{ts}.csv"
	with out_csv.open('w', newline='', encoding='utf-8') as f:
		writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS)
		writer.writeheader()
		for r in records:
			writer.writerow(r.to_row())
	LOGGER.info("Birleştirilmiş CSV: %s (%d kayıt)", out_csv, len(records))
	return out_csv


def write_stats(all_records: List[EmailRecord], out_dir: Path, merged_csv: Path, synthetic_added: int):
	ts = datetime.now().strftime('%Y%m%d_%H%M%S')
	stats_path = out_dir / f"stats_{ts}.json"
	per_account: Dict[str, int] = {}
	senders: Dict[str, int] = {}
	for r in all_records:
		per_account[r.account] = per_account.get(r.account, 0) + 1
		if r.sender_email:
			senders[r.sender_email] = senders.get(r.sender_email, 0) + 1
	data = {
		'total_records': len(all_records),
		'synthetic_records': synthetic_added,
		'accounts': per_account,
		'top_senders': sorted(senders.items(), key=lambda x: x[1], reverse=True)[:10],
		'output_csv': str(merged_csv),
		'generated_at': datetime.now().isoformat(),
	}
	with stats_path.open('w', encoding='utf-8') as f:
		json.dump(data, f, ensure_ascii=False, indent=2)
	LOGGER.info("İstatistikler kaydedildi: %s", stats_path)


def process(metadata_dir: Path, out_dir: Path, synthesize: int) -> None:
	if not metadata_dir.exists():
		raise SystemExit(f"Metadata dizini bulunamadı: {metadata_dir}")
	account_dirs = [p for p in metadata_dir.iterdir() if p.is_dir()]
	if not account_dirs:
		raise SystemExit("Hiç hesap klasörü bulunamadı")
	LOGGER.info("%d hesap klasörü bulundu", len(account_dirs))
	all_records: List[EmailRecord] = []
	for acc_dir in account_dirs:
		latest_csv = find_latest_email_csv(acc_dir)
		if not latest_csv:
			LOGGER.warning("emails_*.csv bulunamadı: %s", acc_dir)
			continue
		records = load_emails(latest_csv, acc_dir.name)
		LOGGER.info("%s -> %d kayıt", acc_dir.name, len(records))
		all_records.extend(records)
	if not all_records:
		raise SystemExit("Hiç kayıt yüklenemedi")
	synthetic_records: List[EmailRecord] = []
	if synthesize:
		synthetic_records = generate_synthetic(all_records, synthesize)
		LOGGER.info("Sentetik kayıt üretildi: %d", len(synthetic_records))
		all_records.extend(synthetic_records)
	merged_csv = write_merged(all_records, out_dir)
	write_stats(all_records, out_dir, merged_csv, len(synthetic_records))


def generate_accounts(metadata_dir: Path, account_count: int, emails_per_account: int, locale: str, inbox_only: bool = False):
	"""Yeni demo hesap klasörleri oluşturup emails_*.csv üretir.

	Her hesap için:
	  - Rastgele email adresi (userX@example.com benzeri faker ile daha gerçekçi)
	  - emails_<timestamp>.csv dosyası
	  - Kolon: id,folder,subject,sender_name,sender_email,delivery_time,size,attachments_count
	"""
	metadata_dir.mkdir(parents=True, exist_ok=True)
	faker = Faker(locale) if _FAKER_AVAILABLE else None
	now_str = datetime.now().strftime('%Y%m%d_%H%M%S')
	for i in range(account_count):
		if faker:
			mailbox = faker.email()
		else:
			mailbox = f"user{i+1}@example.com"
		# Mailbox klasör adı e-postayı aynen kullanabiliriz (özel karakter kısıtlıysa temizle)
		safe_name = mailbox
		account_dir = metadata_dir / safe_name
		account_dir.mkdir(parents=True, exist_ok=True)
		csv_path = account_dir / f"emails_{now_str}.csv"
		with csv_path.open('w', newline='', encoding='utf-8') as f:
			writer = csv.DictWriter(f, fieldnames=EMAIL_CSV_COLUMNS)
			writer.writeheader()
			for j in range(emails_per_account):
				# Sentetik tek email kaydı
				if faker:
					sender_name = faker.name()
					sender_email = faker.email()
					subject = faker.sentence(nb_words=random.randint(3, 8)).rstrip('.')
				else:
					sender_name = f"Sender {j+1}"
					sender_email = f"sender{j+1}@example.com"
					subject = f"Subject {j+1}"
				dt = datetime.now() - timedelta(minutes=random.randint(0, 60*24*30))
				delivery_time = dt.strftime('%Y-%m-%d %H:%M:%S')
				if inbox_only:
					folder = 'Outlook veri dosyasının en üstü/Gelen Kutusu'
				else:
					# Ağırlıklı rastgele klasör seçimi
					folder = random.choices(DEFAULT_FOLDER_POOL, weights=DEFAULT_FOLDER_WEIGHTS, k=1)[0]
				base = f"{subject}{sender_email}{delivery_time}{random.random()}".encode()
				rec_id = hashlib.md5(base).hexdigest()[:16]
				size = random.randint(1_000, 50_000)
				attachments_count = random.choices([0,1,2,3], weights=[0.7,0.2,0.08,0.02])[0]
				writer.writerow({
					'id': rec_id,
					'folder': folder,
					'subject': subject,
					'sender_name': sender_name,
					'sender_email': sender_email,
					'delivery_time': delivery_time,
					'size': size,
					'attachments_count': attachments_count,
				})
		LOGGER.info("Hesap üretildi: %s (%d email)", safe_name, emails_per_account)


def parse_args():
	parser = argparse.ArgumentParser(description="Metadata e-posta CSV birleştirme ve sentetik veri üretimi")
	parser.add_argument('-m', '--metadata-dir', default=os.environ.get('METADATA_DIR', '../extract/metadata'), help='Metadata ana dizini')
	parser.add_argument('-o', '--out-dir', default='./output', help='Çıktı dizini')
	parser.add_argument('-s', '--synthesize', type=int, default=0, help='Üretilecek sentetik kayıt sayısı')
	parser.add_argument('--locale', default='tr_TR', help='Faker locale (faker yüklüyse)')
	parser.add_argument('-v', '--verbose', action='store_true', help='Detaylı log')
	parser.add_argument('--make-accounts', type=int, default=0, help='Yeni demo hesap sayısı (metadata dizininde üret)')
	parser.add_argument('--emails-per-account', type=int, default=100, help='Her hesap için üretilecek email sayısı')
	parser.add_argument('--inbox-only', action='store_true', help='Sadece Gelen Kutusu klasörü kullan (varsayılan: karışık)')
	return parser.parse_args()


def main():
	args = parse_args()
	setup_logging(args.verbose)
	LOGGER.info("Datagen başlıyor ...")
	metadata_dir = Path(args.metadata_dir).resolve()
	out_dir = Path(args.out_dir).resolve()
	if args.synthesize > 0 and not _FAKER_AVAILABLE:
		LOGGER.warning("faker bulunamadı, basit sentetik üretim moduna geçiliyor (daha sınırlı)")
	# Eğer yeni hesaplar üretilecekse önce onları oluştur
	if args.make_accounts > 0:
		if not _FAKER_AVAILABLE:
			LOGGER.warning("Hesap üretimi için faker önerilir; yine de basit modda devam edilecek")
		generate_accounts(metadata_dir, args.make_accounts, args.emails_per_account, args.locale, inbox_only=args.inbox_only)
	process(metadata_dir, out_dir, args.synthesize)
	LOGGER.info("Tamamlandı.")


if __name__ == '__main__':
	main()

