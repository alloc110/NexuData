from datetime import datetime
import json
import logging
import os

class JsonFormatter(logging.Formatter):
      def __init__(self, component_name):
            super().__init__()
            self.component_name = component_name

      def format(self, record):
            log_data = {
                  "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                  "level": record.levelname,
                  "component": self.component_name,  # Nhận dynamic theo từng file
                  "message": record.getMessage()
            }
            if hasattr(record, "extra_data"):
                  log_data.update(record.extra_data)
            return json.dumps(log_data)

def get_logger(logger_name: str, component_name: str) -> logging.Logger:
      """
      Hàm khởi tạo và cấu hình Logger dùng chung cho toàn bộ dự án
      """
      logger = logging.getLogger(logger_name)
      
      # Kiểm tra nếu logger chưa có handler nào thì mới thêm 
      # (Để tránh việc trùng lặp log khi import ở nhiều nơi trong Airflow)
      if not logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(JsonFormatter(component_name))
            logger.addHandler(handler)
            
      log_level = os.getenv("LOG_LEVEL", "INFO")
      logger.setLevel(log_level)
      
      return logger