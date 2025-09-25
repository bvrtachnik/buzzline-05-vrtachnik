"""
consumer_vrtachnik.py

Consume JSON messages from a live data file and insert processed results into SQLite.

Example JSON message:
{
    "message": "I just shared a meme! It was amazing.",
    "author": "Charlie",
    "timestamp": "2025-01-29 14:35:20",
    "category": "humor",
    "sentiment": 0.87,
    "keyword_mentioned": "meme",
    "message_length": 42
}

Environment variables are read via utils.utils_config.
SQLite helpers are in consumers/sqlite_consumer_case.py.
"""

# =========================
# Imports
# =========================
import json
import pathlib
import sys
import time

import utils.utils_config as config
from utils.utils_logger import logger
from .sqlite_consumer_case import init_db, insert_message


# =========================
# Helpers
# =========================
def classify_sentiment(score: float) -> str:
    """Map a numeric sentiment score [-1.0, 1.0] to a readable label."""
    if score <= -0.60:
        return "Very Negative"
    elif score < -0.20:
        return "Negative"
    elif score <= 0.20:
        return "Neutral"
    elif score < 0.60:
        return "Positive"
    else:
        return "Very Positive"


# =========================
# Process a single message
# =========================
def process_message(message: dict) -> dict | None:
    """
    Convert fields to appropriate types and add derived insights.
    Returns a dict ready for insertion or None on error.
    """
    try:
        sentiment = float(message.get("sentiment", 0.0))
        msg_len = int(message.get("message_length", 0))

        processed_message = {
            "message": message.get("message"),
            "author": message.get("author"),
            "timestamp": message.get("timestamp"),
            "category": message.get("category"),
            "sentiment": sentiment,
            "keyword_mentioned": message.get("keyword_mentioned"),
            "message_length": msg_len,

            # Added insights for this project focus:
            "sentiment_label": classify_sentiment(sentiment),
            # Simple intensity proxy: stronger polarity and longer text increase score
            "engagement_score": abs(sentiment) * msg_len,
        }
        logger.info(f"Processed message: {processed_message}")
        return processed_message
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        return None


# =========================
# Consume from live file
# =========================
def consume_messages_from_file(live_data_path: pathlib.Path,
                               sql_path: pathlib.Path,
                               interval_secs: int,
                               last_position: int) -> int:
    """
    Read new lines from the live data file (JSON Lines), process each,
    and write to SQLite. Exits after reaching EOF.
    """
    logger.info("Called consume_messages_from_file() with:")
    logger.info(f"   {live_data_path=}")
    logger.info(f"   {sql_path=}")
    logger.info(f"   {interval_secs=}")
    logger.info(f"   {last_position=}")

    logger.info("1. Initialize the database.")
    init_db(sql_path)

    logger.info("2. Set the last position to 0 to start at the beginning of the file.")
    last_position = 0

    while True:
        try:
            logger.info(f"3. Read from live data file at position {last_position}.")
            with open(live_data_path, "r", encoding="utf-8") as file:
                file.seek(last_position)
                for line in file:
                    if not line.strip():
                        continue
                    message = json.loads(line.strip())
                    processed_message = process_message(message)
                    if processed_message:
                        insert_message(processed_message, sql_path)

                # EOF reached, remember position and return
                last_position = file.tell()
                return last_position

        except FileNotFoundError:
            logger.error(f"ERROR: Live data file not found at {live_data_path}.")
            sys.exit(10)
        except Exception as e:
            logger.error(f"ERROR: Error reading from live data file: {e}")
            sys.exit(11)

        time.sleep(interval_secs)


# =========================
# Main
# =========================
def main():
    """Read config, initialize DB, then consume and store messages."""
    logger.info("Starting consumer_vrtachnik.")
    logger.info("Using utils_config for environment variables.")

    logger.info("STEP 1. Read environment variables.")
    try:
        interval_secs: int = config.get_message_interval_seconds_as_int()
        live_data_path: pathlib.Path = config.get_live_data_path()
        sqlite_path: pathlib.Path = config.get_sqlite_path()
        logger.info("SUCCESS: Read environment variables.")
    except Exception as e:
        logger.error(f"ERROR: Failed to read environment variables: {e}")
        sys.exit(1)

    logger.info("STEP 2. Delete any prior database file for a fresh start.")
    if sqlite_path.exists():
        try:
            sqlite_path.unlink()
            logger.info("SUCCESS: Deleted database file.")
        except Exception as e:
            logger.error(f"ERROR: Failed to delete DB file: {e}")
            sys.exit(2)

    logger.info("STEP 3. Initialize a new database with an empty table.")
    try:
        init_db(sqlite_path)
    except Exception as e:
        logger.error(f"ERROR: Failed to create db table: {e}")
        sys.exit(3)

    logger.info("STEP 4. Begin consuming and storing messages.")
    try:
        consume_messages_from_file(live_data_path, sqlite_path, interval_secs, 0)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"ERROR: Unexpected error: {e}")
    finally:
        logger.info("TRY/FINALLY: Consumer shutting down.")


if __name__ == "__main__":
    main()
