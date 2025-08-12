import os
import sys
import json
import datetime
import hashlib
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
import logging

try:
    import pypff
except ImportError:
    print("HATA: pypff kütüphanesi bulunamadı!")
    print("Kurulum için: pip install pypff")
    print("Veya: conda install -c conda-forge pypff")
    sys.exit(1)


class PSTAnalyzer:
    """
    .pst dosyalarını analiz eden ana sınıf
    """
    
    def __init__(self, pst_file_path: str, output_dir: str = None):
        """
        PSTAnalyzer başlatıcı
        
        Args:
            pst_file_path (str): .pst dosyasının yolu
            output_dir (str): Çıktı dizini (varsayılan: pst dosyası yanında)
        """
        self.pst_file_path = Path(pst_file_path)
        self.output_dir = Path(output_dir) if output_dir else self.pst_file_path.parent / "pst_analysis"
        self.pst_file = None
        
        # Çıktı dizinini oluştur
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Logging yapılandırması
        self._setup_logging()
        
        # Analiz sonuçları
        self.analysis_results = {
            'emails': [],
            'contacts': [],
            'calendar': [],
            'tasks': [],
            'notes': [],
            'journal': [],
            'attachments': [],
            'statistics': {}
        }
    
    def _setup_logging(self):
        """Logging yapılandırması"""
        log_file = self.output_dir / f"pst_analysis_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def open_pst_file(self) -> bool:
        """
        .pst dosyasını açar
        
        Returns:
            bool: Başarılı ise True
        """
        try:
            if not self.pst_file_path.exists():
                self.logger.error(f"PST dosyası bulunamadı: {self.pst_file_path}")
                return False
            
            self.pst_file = pypff.file()
            self.pst_file.open(str(self.pst_file_path))
            
            self.logger.info(f"PST dosyası başarıyla açıldı: {self.pst_file_path}")
            
            # PST dosya bilgilerini güvenli şekilde al
            try:
                if hasattr(self.pst_file, 'format_version'):
                    self.logger.info(f"PST dosya formatı: {self.pst_file.format_version}")
                if hasattr(self.pst_file, 'content_type'):
                    self.logger.info(f"PST dosya türü: {self.pst_file.content_type}")
                if hasattr(self.pst_file, 'size'):
                    self.logger.info(f"PST dosya boyutu: {self.pst_file.size:,} byte")
            except Exception as info_error:
                self.logger.debug(f"PST dosya bilgisi alınamadı: {info_error}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"PST dosyası açılamadı: {e}")
            return False
    
    def close_pst_file(self):
        """PST dosyasını kapatır"""
        if self.pst_file:
            self.pst_file.close()
            self.logger.info("PST dosyası kapatıldı")
    
    def extract_emails(self, folder=None, parent_path="") -> List[Dict]:
        """
        E-postaları çıkarır
        
        Args:
            folder: Analiz edilecek klasör (None ise root)
            parent_path: Üst klasör yolu
            
        Returns:
            List[Dict]: E-posta listesi
        """
        emails = []
        
        try:
            if folder is None:
                folder = self.pst_file.root_folder
                self.logger.info("E-posta çıkarma işlemi başlatıldı...")
            
            # Alt klasörleri işle
            for sub_folder in folder.sub_folders:
                folder_path = f"{parent_path}/{sub_folder.name}" if parent_path else sub_folder.name
                self.logger.info(f"Klasör işleniyor: {folder_path}")
                emails.extend(self.extract_emails(sub_folder, folder_path))
            
            # Mesajları işle
            for message in folder.sub_messages:
                try:
                    email_data = self._extract_single_email(message, parent_path)
                    if email_data:
                        emails.append(email_data)
                        
                except Exception as e:
                    self.logger.warning(f"E-posta işlenirken hata: {e}")
                    continue
            
        except Exception as e:
            self.logger.error(f"E-posta çıkarma hatası: {e}")
        
        return emails
    
    def _extract_single_email(self, message, folder_path: str) -> Optional[Dict]:
        """
        Tek bir e-postayı işler
        
        Args:
            message: pypff message objesi
            folder_path: Klasör yolu
            
        Returns:
            Optional[Dict]: E-posta verisi
        """
        try:
            # E-posta ID'si oluştur
            email_id = self._generate_email_id(message)
            
            # E-posta verilerini güvenli şekilde al
            subject = getattr(message, 'subject', '')
            if isinstance(subject, bytes):
                subject = subject.decode('utf-8', errors='ignore')
            
            sender_name = getattr(message, 'sender_name', '')
            if isinstance(sender_name, bytes):
                sender_name = sender_name.decode('utf-8', errors='ignore')
            
            sender_email = getattr(message, 'sender_email_address', '')
            if isinstance(sender_email, bytes):
                sender_email = sender_email.decode('utf-8', errors='ignore')
            
            body_plain = getattr(message, 'plain_text_body', '')
            if isinstance(body_plain, bytes):
                body_plain = body_plain.decode('utf-8', errors='ignore')
            
            body_html = getattr(message, 'html_body', '')
            if isinstance(body_html, bytes):
                body_html = body_html.decode('utf-8', errors='ignore')
            
            email_data = {
                'id': email_id,
                'folder': folder_path,
                'subject': subject,
                'sender_name': sender_name,
                'sender_email': sender_email,
                'recipients': self._extract_recipients(message),
                'delivery_time': self._format_datetime(getattr(message, 'delivery_time', None)),
                'creation_time': self._format_datetime(getattr(message, 'creation_time', None)),
                'modification_time': self._format_datetime(getattr(message, 'modification_time', None)),
                'size': getattr(message, 'size', 0),
                'body_plain': body_plain[:1000] if body_plain else '',  # İlk 1000 karakter
                'body_html': body_html[:1000] if body_html else '',     # İlk 1000 karakter
                'message_class': str(getattr(message, 'message_class', '')),
                'priority': str(getattr(message, 'priority', '')),
                'importance': str(getattr(message, 'importance', '')),
                'attachments': self._extract_attachments(message, email_id),
                'categories': str(getattr(message, 'categories', '')),
                'read_flag': getattr(message, 'is_read', False)
            }
            
            return email_data
            
        except Exception as e:
            self.logger.warning(f"E-posta veri çıkarma hatası: {e}")
            return None
    
    def _extract_recipients(self, message) -> List[Dict]:
        """E-posta alıcılarını çıkarır"""
        recipients = []
        
        try:
            # Recipients listesini kontrol et
            if hasattr(message, 'recipients'):
                for recipient in message.recipients:
                    recipient_data = {
                        'name': getattr(recipient, 'name', ''),
                        'email': getattr(recipient, 'email_address', ''),
                        'type': getattr(recipient, 'type', '')  # TO, CC, BCC
                    }
                    recipients.append(recipient_data)
        except Exception as e:
            self.logger.debug(f"Alıcı bilgisi çıkarılamadı: {e}")
        
        return recipients
    
    def _extract_attachments(self, message, email_id: str) -> List[Dict]:
        """E-posta eklerini çıkarır"""
        attachments = []
        
        try:
            if hasattr(message, 'attachments'):
                for i, attachment in enumerate(message.attachments):
                    attachment_data = {
                        'email_id': email_id,
                        'index': i,
                        'name': getattr(attachment, 'name', f'attachment_{i}'),
                        'size': getattr(attachment, 'size', 0),
                        'type': getattr(attachment, 'attachment_type', ''),
                        'saved_path': None
                    }
                    
                    # Ek dosyayı kaydet
                    saved_path = self._save_attachment(attachment, email_id, i)
                    if saved_path:
                        attachment_data['saved_path'] = str(saved_path)
                    
                    attachments.append(attachment_data)
                    self.analysis_results['attachments'].append(attachment_data)
        
        except Exception as e:
            self.logger.debug(f"Ek dosya çıkarma hatası: {e}")
        
        return attachments
    
    def _save_attachment(self, attachment, email_id: str, index: int) -> Optional[Path]:
        """Ek dosyayı kaydeder"""
        try:
            attachments_dir = self.output_dir / "attachments" / email_id
            attachments_dir.mkdir(parents=True, exist_ok=True)
            
            filename = getattr(attachment, 'name', f'attachment_{index}')
            # Güvenli dosya adı oluştur
            safe_filename = "".join(c for c in filename if c.isalnum() or c in (' ', '-', '_', '.')).rstrip()
            if not safe_filename:
                safe_filename = f'attachment_{index}'
            
            file_path = attachments_dir / safe_filename
            
            # Dosya verilerini al ve kaydet
            if hasattr(attachment, 'data'):
                data = attachment.data
                if data:
                    with open(file_path, 'wb') as f:
                        f.write(data)
                    return file_path
            
        except Exception as e:
            self.logger.warning(f"Ek dosya kaydetme hatası: {e}")
        
        return None
    
    def extract_contacts(self) -> List[Dict]:
        """Kişi bilgilerini çıkarır"""
        contacts = []
        self.logger.info("Kişi bilgileri çıkarılıyor...")
        
        try:
            # Contacts klasörünü bul
            contacts_folder = self._find_folder_by_name("Contacts")
            if not contacts_folder:
                self.logger.warning("Contacts klasörü bulunamadı")
                return contacts
            
            for message in contacts_folder.sub_messages:
                try:
                    contact_data = {
                        'display_name': getattr(message, 'subject', ''),
                        'email_address': getattr(message, 'sender_email_address', ''),
                        'business_phone': '',
                        'home_phone': '',
                        'mobile_phone': '',
                        'company': '',
                        'job_title': '',
                        'creation_time': self._format_datetime(getattr(message, 'creation_time', None)),
                        'modification_time': self._format_datetime(getattr(message, 'modification_time', None))
                    }
                    
                    # MAPI özelliklerinden daha fazla bilgi al
                    if hasattr(message, 'properties'):
                        for prop in message.properties:
                            # Telefon numaraları, şirket bilgileri vb.
                            pass  # MAPI özellik kodları ile genişletilebilir
                    
                    contacts.append(contact_data)
                    
                except Exception as e:
                    self.logger.warning(f"Kişi işleme hatası: {e}")
                    continue
        
        except Exception as e:
            self.logger.error(f"Kişi çıkarma hatası: {e}")
        
        return contacts
    
    def extract_calendar(self) -> List[Dict]:
        """Takvim kayıtlarını çıkarır"""
        calendar_events = []
        self.logger.info("Takvim kayıtları çıkarılıyor...")
        
        try:
            # Calendar klasörünü bul
            calendar_folder = self._find_folder_by_name("Calendar")
            if not calendar_folder:
                self.logger.warning("Calendar klasörü bulunamadı")
                return calendar_events
            
            for message in calendar_folder.sub_messages:
                try:
                    event_data = {
                        'subject': getattr(message, 'subject', ''),
                        'location': '',
                        'start_time': self._format_datetime(getattr(message, 'creation_time', None)),
                        'end_time': '',
                        'organizer': getattr(message, 'sender_name', ''),
                        'attendees': [],
                        'body': getattr(message, 'plain_text_body', ''),
                        'importance': getattr(message, 'importance', ''),
                        'creation_time': self._format_datetime(getattr(message, 'creation_time', None))
                    }
                    
                    # Takvim özel özelliklerini çıkar
                    if hasattr(message, 'properties'):
                        for prop in message.properties:
                            # Başlangıç/bitiş zamanları, katılımcılar vb.
                            pass  # Genişletilebilir
                    
                    calendar_events.append(event_data)
                    
                except Exception as e:
                    self.logger.warning(f"Takvim kaydı işleme hatası: {e}")
                    continue
        
        except Exception as e:
            self.logger.error(f"Takvim çıkarma hatası: {e}")
        
        return calendar_events
    
    def extract_tasks(self) -> List[Dict]:
        """Görev listesini çıkarır"""
        tasks = []
        self.logger.info("Görevler çıkarılıyor...")
        
        try:
            # Tasks klasörünü bul
            tasks_folder = self._find_folder_by_name("Tasks")
            if not tasks_folder:
                self.logger.warning("Tasks klasörü bulunamadı")
                return tasks
            
            for message in tasks_folder.sub_messages:
                try:
                    task_data = {
                        'subject': getattr(message, 'subject', ''),
                        'body': getattr(message, 'plain_text_body', ''),
                        'status': '',
                        'priority': getattr(message, 'priority', ''),
                        'due_date': '',
                        'start_date': '',
                        'completion_date': '',
                        'percent_complete': 0,
                        'creation_time': self._format_datetime(getattr(message, 'creation_time', None))
                    }
                    
                    tasks.append(task_data)
                    
                except Exception as e:
                    self.logger.warning(f"Görev işleme hatası: {e}")
                    continue
        
        except Exception as e:
            self.logger.error(f"Görev çıkarma hatası: {e}")
        
        return tasks
    
    def extract_notes(self) -> List[Dict]:
        """Notları çıkarır"""
        notes = []
        self.logger.info("Notlar çıkarılıyor...")
        
        try:
            # Notes klasörünü bul
            notes_folder = self._find_folder_by_name("Notes")
            if not notes_folder:
                self.logger.warning("Notes klasörü bulunamadı")
                return notes
            
            for message in notes_folder.sub_messages:
                try:
                    note_data = {
                        'subject': getattr(message, 'subject', ''),
                        'body': getattr(message, 'plain_text_body', ''),
                        'creation_time': self._format_datetime(getattr(message, 'creation_time', None)),
                        'modification_time': self._format_datetime(getattr(message, 'modification_time', None)),
                        'color': '',
                        'size': getattr(message, 'size', 0)
                    }
                    
                    notes.append(note_data)
                    
                except Exception as e:
                    self.logger.warning(f"Not işleme hatası: {e}")
                    continue
        
        except Exception as e:
            self.logger.error(f"Not çıkarma hatası: {e}")
        
        return notes
    
    def extract_journal(self) -> List[Dict]:
        """Günlük kayıtlarını çıkarır"""
        journal_entries = []
        self.logger.info("Günlük kayıtları çıkarılıyor...")
        
        try:
            # Journal klasörünü bul
            journal_folder = self._find_folder_by_name("Journal")
            if not journal_folder:
                self.logger.warning("Journal klasörü bulunamadı")
                return journal_entries
            
            for message in journal_folder.sub_messages:
                try:
                    journal_data = {
                        'subject': getattr(message, 'subject', ''),
                        'body': getattr(message, 'plain_text_body', ''),
                        'entry_type': '',
                        'start_time': self._format_datetime(getattr(message, 'creation_time', None)),
                        'duration': 0,
                        'companies': '',
                        'contacts': '',
                        'creation_time': self._format_datetime(getattr(message, 'creation_time', None))
                    }
                    
                    journal_entries.append(journal_data)
                    
                except Exception as e:
                    self.logger.warning(f"Günlük kaydı işleme hatası: {e}")
                    continue
        
        except Exception as e:
            self.logger.error(f"Günlük çıkarma hatası: {e}")
        
        return journal_entries
    
    def _find_folder_by_name(self, folder_name: str, folder=None):
        """Belirtilen isimde klasör bulur"""
        if folder is None:
            folder = self.pst_file.root_folder
        
        # Klasör ismini güvenli şekilde kontrol et
        try:
            folder_name_safe = getattr(folder, 'name', '')
            if folder_name_safe and folder_name_safe.lower() == folder_name.lower():
                return folder
        except (AttributeError, TypeError):
            pass
        
        try:
            for sub_folder in folder.sub_folders:
                found = self._find_folder_by_name(folder_name, sub_folder)
                if found:
                    return found
        except (AttributeError, TypeError):
            pass
        
        return None
    
    def generate_statistics(self):
        """Analiz istatistiklerini oluşturur"""
        stats = {
            'total_emails': len(self.analysis_results['emails']),
            'total_contacts': len(self.analysis_results['contacts']),
            'total_calendar_events': len(self.analysis_results['calendar']),
            'total_tasks': len(self.analysis_results['tasks']),
            'total_notes': len(self.analysis_results['notes']),
            'total_journal_entries': len(self.analysis_results['journal']),
            'total_attachments': len(self.analysis_results['attachments']),
            'analysis_date': datetime.datetime.now().isoformat(),
            'pst_file': str(self.pst_file_path),
            'pst_file_size': self.pst_file_path.stat().st_size if self.pst_file_path.exists() else 0
        }
        
        # E-posta istatistikleri
        if self.analysis_results['emails']:
            # Tarih aralığı
            dates = [email['delivery_time'] for email in self.analysis_results['emails'] if email['delivery_time']]
            if dates:
                stats['email_date_range'] = {
                    'earliest': min(dates),
                    'latest': max(dates)
                }
            
            # En fazla e-posta gönderen
            senders = {}
            for email in self.analysis_results['emails']:
                sender = email.get('sender_email', 'unknown')
                senders[sender] = senders.get(sender, 0) + 1
            
            if senders:
                stats['top_senders'] = sorted(senders.items(), key=lambda x: x[1], reverse=True)[:10]
        
        self.analysis_results['statistics'] = stats
    
    def save_results(self, format_type: str = 'json'):
        """Analiz sonuçlarını kaydeder"""
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = None
        
        if format_type.lower() == 'json':
            output_file = self.output_dir / f"pst_analysis_{timestamp}.json"
            
            clean_results = self._clean_for_json(self.analysis_results)
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(clean_results, f, ensure_ascii=False, indent=2)
            
            self.logger.info(f"JSON sonuçları kaydedildi: {output_file}")
        
        # CSV sonuçlarını kaydet
        self._save_csv_results(timestamp)
        
        return output_file
    
    def _save_csv_results(self, timestamp: str):
        """CSV formatında sonuçları kaydeder"""
        import csv
        
        # E-postalar CSV
        if self.analysis_results['emails']:
            csv_file = self.output_dir / f"emails_{timestamp}.csv"
            with open(csv_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=[
                    'id', 'folder', 'subject', 'sender_name', 'sender_email',
                    'delivery_time', 'size', 'attachments_count'
                ])
                writer.writeheader()
                for email in self.analysis_results['emails']:
                    writer.writerow({
                        'id': email['id'],
                        'folder': email['folder'],
                        'subject': email['subject'],
                        'sender_name': email['sender_name'],
                        'sender_email': email['sender_email'],
                        'delivery_time': email['delivery_time'],
                        'size': email['size'],
                        'attachments_count': len(email['attachments'])
                    })
            
            self.logger.info(f"E-posta CSV kaydedildi: {csv_file}")
    
    def perform_full_analysis(self) -> bool:
        """Tam analiz gerçekleştirir"""
        try:
            self.logger.info("=== PST DOSYASI TAM ANALİZİ BAŞLATILIYOR ===")
            
            # PST dosyasını aç
            if not self.open_pst_file():
                return False
            
            # Tüm verileri çıkar
            self.analysis_results['emails'] = self.extract_emails()
            self.analysis_results['contacts'] = self.extract_contacts()
            self.analysis_results['calendar'] = self.extract_calendar()
            self.analysis_results['tasks'] = self.extract_tasks()
            self.analysis_results['notes'] = self.extract_notes()
            self.analysis_results['journal'] = self.extract_journal()
            
            # İstatistikleri oluştur
            self.generate_statistics()
            
            # Sonuçları kaydet
            output_file = self.save_results()
            
            # PST dosyasını kapat
            self.close_pst_file()
            
            # Başarı mesajı
            self.logger.info(f"=== ANALİZ TAMAMLANDI ===")
            self.logger.info(f"Çıktı dosyaları: {self.output_dir}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Analiz hatası: {e}")
            if self.pst_file:
                self.close_pst_file()
            return False
    
    # Yardımcı metodlar
    def _generate_email_id(self, message) -> str:
        """E-posta için benzersiz ID oluşturur"""
        try:
            # Subject, sender ve creation time'dan hash oluştur
            content = f"{getattr(message, 'subject', '')}{getattr(message, 'sender_email_address', '')}{getattr(message, 'creation_time', '')}"
            return hashlib.md5(content.encode()).hexdigest()[:16]
        except:
            return f"email_{datetime.datetime.now().timestamp()}"
    
    def _format_datetime(self, dt) -> str:
        """Datetime objesini string'e çevirir"""
        if dt is None:
            return ""
        try:
            if hasattr(dt, 'strftime'):
                return dt.strftime('%Y-%m-%d %H:%M:%S')
            return str(dt)
        except:
            return str(dt)
    
    def _clean_for_json(self, data):
        """JSON serileştirme için veriyi temizler"""
        if isinstance(data, bytes):
            try:
                return data.decode('utf-8', errors='ignore')
            except:
                return str(data)
        elif isinstance(data, dict):
            return {key: self._clean_for_json(value) for key, value in data.items()}
        elif isinstance(data, list):
            return [self._clean_for_json(item) for item in data]
        elif hasattr(data, '__dict__'):
            return str(data)
        else:
            return data


def analyze_pst_file(pst_file_path: str, output_dir: str = None) -> bool:
    """
    PST dosyasını analiz eden ana fonksiyon
    
    Args:
        pst_file_path (str): .pst dosyasının yolu
        output_dir (str): Çıktı dizini
        
    Returns:
        bool: Başarılı ise True
    """
    analyzer = PSTAnalyzer(pst_file_path, output_dir)
    return analyzer.perform_full_analysis()


def analyze_directory(directory_path: str, output_dir: str = None) -> List[str]:
    """
    Dizindeki tüm .pst dosyalarını analiz eder
    
    Args:
        directory_path (str): .pst dosyalarının bulunduğu dizin
        output_dir (str): Çıktı dizini
        
    Returns:
        List[str]: İşlenen dosya listesi
    """
    directory = Path(directory_path)
    processed_files = []
    
    if not directory.exists():
        print(f"Dizin bulunamadı: {directory}")
        return processed_files
    
    # .pst dosyalarını bul
    pst_files = list(directory.glob("*.pst"))
    
    if not pst_files:
        print(f"Dizinde .pst dosyası bulunamadı: {directory}")
        return processed_files
    
    print(f"{len(pst_files)} adet .pst dosyası bulundu")
    
    for pst_file in pst_files:
        print(f"\nİşleniyor: {pst_file.name}")
        
        # Her dosya için ayrı çıktı dizini
        file_output_dir = Path(output_dir) / pst_file.stem if output_dir else pst_file.parent / f"{pst_file.stem}_analysis"
        
        success = analyze_pst_file(str(pst_file), str(file_output_dir))
        
        if success:
            processed_files.append(str(pst_file))
            print(f"✓ Başarıyla işlendi: {pst_file.name}")
        else:
            print(f"✗ İşlenemedi: {pst_file.name}")
    
    return processed_files


if __name__ == "__main__":
    data_directory = "data"
    output_directory = "metadata"
    processed = analyze_directory(data_directory, output_directory)
