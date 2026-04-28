import os
import sys
import yaml
import logging
import zipfile
import tarfile
from datetime import datetime
from pathlib import Path
from typing import Optional
import requests
import schedule
import time
import threading

class BackupBot:
    def __init__(self, config_path: str = "config.yaml"):
        self.config = self._load_config(config_path)
        self._setup_logging()
        self.logger = logging.getLogger(__name__)
        self.last_update_id = 0
        self.running = True
        self.logger.info("BackupBot запущен")

    def _load_config(self, config_path: str) -> dict:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        required = ['telegram.bot_token', 'telegram.chat_id', 
                   'backup.source_path', 'backup.storage_path']
        for field in required:
            keys = field.split('.')
            value = config
            for key in keys:
                if key not in value:
                    raise ValueError(f"Отсутствует поле в конфиге: {field}")
                value = value[key]
        
        return config

    def _setup_logging(self):
        log_config = self.config.get('logging', {})
        level = getattr(logging, log_config.get('level', 'INFO'))
        
        logging.basicConfig(
            level=level,
            format='%(asctime)s [%(levelname)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
            handlers=[
                logging.FileHandler(log_config.get('file', 'backup.log'), encoding='utf-8'),
                logging.StreamHandler()
            ]
        )

    def _api_url(self, method: str) -> str:
        base = self.config['telegram']['api_base_url']
        token = self.config['telegram']['bot_token']
        return f"{base}/bot{token}/{method}"

    def send_message(self, text: str) -> bool:
        try:
            url = self._api_url('sendMessage')
            data = {
                'chat_id': self.config['telegram']['chat_id'],
                'text': text,
                'parse_mode': 'HTML'
            }
            resp = requests.post(url, json=data, timeout=30)
            if resp.status_code == 200:
                self.logger.info(f"Сообщение отправлено: {text[:50]}...")
                return True
            else:
                self.logger.error(f"Ошибка отправки сообщения: {resp.status_code} {resp.text}")
                return False
        except Exception as e:
            self.logger.error(f"Ошибка Telegram: {e}")
            return False

    def get_updates(self, offset: Optional[int] = None):
        try:
            url = self._api_url('getUpdates')
            params = {'timeout': 30, 'offset': offset}
            resp = requests.get(url, params=params, timeout=35)
            return resp.json().get('result', []) if resp.status_code == 200 else []
        except:
            return []

    def create_backup(self) -> Optional[Path]:
        """Создаёт архив, кладёт в storage_path, чистит старые"""
        try:
            src = Path(self.config['backup']['source_path'])
            storage = Path(self.config['backup']['storage_path'])
            storage.mkdir(parents=True, exist_ok=True)

            prefix = self.config['backup'].get('archive_name_prefix', 'backup')
            compression = self.config['backup'].get('compression', 'zip')
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

            # Имя архива
            if compression == 'zip':
                archive_name = f"{prefix}_{timestamp}.zip"
            elif compression in ('tar.gz', 'tar.bz2'):
                ext = compression.replace('.', '')
                archive_name = f"{prefix}_{timestamp}.tar.{ext}"
            else:
                raise ValueError(f"Неизвестный формат: {compression}")

            archive_path = storage / archive_name

            # Создание архива
            self.logger.info(f"Создаю архив: {archive_path}")
            if compression == 'zip':
                self._zip_folder(src, archive_path)
            else:
                self._tar_folder(src, archive_path, compression)

            size_mb = archive_path.stat().st_size / 1024 / 1024
            self.logger.info(f"Архив готов: {archive_path} ({size_mb:.2f} MB)")

            # Чистим старые бекапы
            self._cleanup_old_backups(storage, prefix)

            return archive_path

        except Exception as e:
            self.logger.error(f"Ошибка создания архива: {e}")
            return None

    def _zip_folder(self, src: Path, dst: Path):
        with zipfile.ZipFile(dst, 'w', zipfile.ZIP_DEFLATED) as zf:
            for f in src.rglob('*'):
                if f.is_file():
                    zf.write(f, f.relative_to(src.parent))

    def _tar_folder(self, src: Path, dst: Path, compression: str):
        mode = 'w:gz' if compression == 'tar.gz' else 'w:bz2'
        with tarfile.open(dst, mode) as tf:
            tf.add(src, arcname=src.name)

    def _cleanup_old_backups(self, storage: Path, prefix: str):
        """Оставляет только max_backups последних архивов"""
        max_backups = self.config['backup'].get('max_backups', 3)
        compression = self.config['backup'].get('compression', 'zip')
        
        if compression == 'zip':
            pattern = f"{prefix}_*.zip"
        else:
            pattern = f"{prefix}_*.tar.*"
        
        backups = sorted(storage.glob(pattern), key=os.path.getmtime, reverse=True)
        
        if len(backups) > max_backups:
            for old in backups[max_backups:]:
                old.unlink()
                self.logger.info(f"Удалён старый бекап: {old.name}")

    def process_commands(self):
        self.logger.info("Слушаю команды...")
        while self.running:
            updates = self.get_updates(offset=self.last_update_id + 1)
            for upd in updates:
                self.last_update_id = upd['update_id']
                msg = upd.get('message', {})
                text = msg.get('text', '')
                chat_id = msg.get('chat', {}).get('id')

                if str(chat_id) != str(self.config['telegram']['chat_id']):
                    self.send_message("⛔ Нет доступа")
                    continue

                if text == '/backup':
                    threading.Thread(target=self.do_backup, args=(True,)).start()
                elif text == '/status':
                    self.cmd_status()
                elif text in ('/start', '/help'):
                    self.send_message(
                        "🤖 <b>Backup Bot</b>\n"
                        "/backup — запустить сейчас\n"
                        "/status — статистика\n"
                    )
            time.sleep(1)

    def cmd_status(self):
        storage = Path(self.config['backup']['storage_path'])
        prefix = self.config['backup'].get('archive_name_prefix', 'backup')
        compression = self.config['backup'].get('compression', 'zip')
        
        if compression == 'zip':
            pattern = f"{prefix}_*.zip"
        else:
            pattern = f"{prefix}_*.tar.*"
        
        backups = sorted(storage.glob(pattern), key=os.path.getmtime, reverse=True)
        
        lines = [f"📦 <b>Бекапов: {len(backups)}/{self.config['backup']['max_backups']}</b>"]
        for b in backups:
            size_mb = b.stat().st_size / 1024 / 1024
            mtime = datetime.fromtimestamp(b.stat().st_mtime).strftime('%d.%m.%Y %H:%M')
            lines.append(f"• {b.name} — {size_mb:.1f} MB ({mtime})")
        
        self.send_message('\n'.join(lines))

    def do_backup(self, manual: bool = False):
        if manual:
            self.send_message("🔄 <b>Запускаю бекап...</b>")

        archive = self.create_backup()
        
        if archive:
            size_mb = archive.stat().st_size / 1024 / 1024
            storage = Path(self.config['backup']['storage_path'])
            prefix = self.config['backup'].get('archive_name_prefix', 'backup')
            compression = self.config['backup'].get('compression', 'zip')
            
            if compression == 'zip':
                pattern = f"{prefix}_*.zip"
            else:
                pattern = f"{prefix}_*.tar.*"
            
            backups = sorted(storage.glob(pattern), key=os.path.getmtime, reverse=True)
            
            msg = (
                f"✅ <b>Бекап выполнен</b>\n"
                f"📁 Файл: <code>{archive.name}</code>\n"
                f"📏 Размер: {size_mb:.2f} MB\n"
                f"📦 Хранится: {len(backups)}/{self.config['backup']['max_backups']}\n"
                f"🕒 {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}"
            )
            self.send_message(msg)
        else:
            self.send_message("❌ <b>Ошибка бекапа!</b> Проверьте логи.")

    def start(self):
        sched = self.config.get('schedule', {})
        
        if sched.get('enabled', False):
            interval = sched.get('interval_hours', 24)
            start_time = sched.get('start_time', '03:00')
            schedule.every().day.at(start_time).do(self.do_backup)
            self.logger.info(f"Автобекап: каждый день в {start_time}")
            self.send_message(f"🤖 Бот запущен. Автобекап ежедневно в {start_time}.")
        else:
            self.logger.info("Автобекап выключен. Работаю по командам.")
            self.send_message("🤖 Бот запущен в ручном режиме.\nИспользуйте /backup")

        # Обработчик команд в фоне
        threading.Thread(target=self.process_commands, daemon=True).start()

        while self.running:
            schedule.run_pending()
            time.sleep(30)

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--once', action='store_true', help='Один бекап и выход')
    parser.add_argument('--config', default='config.yaml')
    args = parser.parse_args()

    bot = BackupBot(args.config)

    if args.once:
        bot.do_backup()
        return

    try:
        bot.start()
    except KeyboardInterrupt:
        print("\nОстановлено.")
        bot.running = False

if __name__ == '__main__':
    main()