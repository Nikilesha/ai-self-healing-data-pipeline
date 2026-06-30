import os
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


def save_rollback_batch(df, reason,stage,processed_rows):

    rollback_folder = os.getenv("ROLLBACK_DATA_PATH")

    os.makedirs(rollback_folder, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    file_path = os.path.join(
        rollback_folder,
        f"rollback_{timestamp}.csv"
    )

    df.to_csv(file_path, index=False)

    logger.error("=" * 50)
    logger.error("DATABASE ROLLBACK EXECUTED")
    logger.error(f"Stage          : {stage}")
    logger.error(f"Rows Attempted : {processed_rows}")
    logger.error(f"Reason         : {reason}")
    logger.error(f"Rollback File  : {os.path.basename(file_path)}")
    logger.error(f"Timestamp      : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.error("=" * 50)

    return file_path