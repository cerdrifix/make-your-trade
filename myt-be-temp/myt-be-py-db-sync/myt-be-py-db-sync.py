#!/usr/bin/env python3
"""
MTG Cards Database Application
Reads Scryfall JSON card data and saves it to MySQL database using stored procedures.
"""

import json
import mariadb
from mariadb import Error
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
import argparse
import sys
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MTGCardDatabase:
    """Handles database operations for MTG cards."""
    
    def __init__(self, host: str, database: str, user: str, password: str, port: int = 3306):
        """Initialize database connection parameters."""
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = port
        self.connection = None
    
    def connect(self) -> bool:
        """Establish database connection."""
        try:
            self.connection = mariadb.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password,
                port=self.port,
                autocommit=False
            )
            logger.info(f"Successfully connected to MySQL database: {self.database}")
            return True
        except Error as e:
            logger.error(f"Error connecting to MySQL: {e}")
            return False
    
    def disconnect(self):
        """Close database connection."""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logger.info("MySQL connection closed")
    
    def parse_date(self, date_str: Optional[str]) -> Optional[str]:
        """Parse date string to MySQL format."""
        if not date_str:
            return None
        try:
            return datetime.strptime(date_str, '%Y-%m-%d').date()
        except ValueError:
            logger.warning(f"Invalid date format: {date_str}")
            return None
    
    def safe_get(self, data: Dict, key: str, default=None):
        """Safely get value from dictionary."""
        return data.get(key, default)
    
    def prepare_card_data(self, card_data: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare card data for database insertion."""
        return {
            'id': self.safe_get(card_data, 'id'),
            'oracle_id': self.safe_get(card_data, 'oracle_id'),
            'multiverse_ids': json.dumps(self.safe_get(card_data, 'multiverse_ids', [])),
            'mtgo_id': self.safe_get(card_data, 'mtgo_id'),
            'arena_id': self.safe_get(card_data, 'arena_id'),
            'tcgplayer_id': self.safe_get(card_data, 'tcgplayer_id'),
            'cardmarket_id': self.safe_get(card_data, 'cardmarket_id'),
            'name': self.safe_get(card_data, 'name'),
            'lang': self.safe_get(card_data, 'lang'),
            'released_at': self.parse_date(self.safe_get(card_data, 'released_at')),
            'uri': self.safe_get(card_data, 'uri'),
            'scryfall_uri': self.safe_get(card_data, 'scryfall_uri'),
            'layout': self.safe_get(card_data, 'layout'),
            'highres_image': self.safe_get(card_data, 'highres_image', False),
            'image_status': self.safe_get(card_data, 'image_status'),
            'image_uris': json.dumps(self.safe_get(card_data, 'image_uris', {})),
            'mana_cost': self.safe_get(card_data, 'mana_cost'),
            'cmc': self.safe_get(card_data, 'cmc'),
            'type_line': self.safe_get(card_data, 'type_line'),
            'oracle_text': self.safe_get(card_data, 'oracle_text'),
            'colors': json.dumps(self.safe_get(card_data, 'colors', [])),
            'color_identity': json.dumps(self.safe_get(card_data, 'color_identity', [])),
            'keywords': json.dumps(self.safe_get(card_data, 'keywords', [])),
            'produced_mana': json.dumps(self.safe_get(card_data, 'produced_mana', [])),
            'legalities': json.dumps(self.safe_get(card_data, 'legalities', {})),
            'games': json.dumps(self.safe_get(card_data, 'games', [])),
            'reserved': self.safe_get(card_data, 'reserved', False),
            'game_changer': self.safe_get(card_data, 'game_changer', False),
            'foil': self.safe_get(card_data, 'foil', False),
            'nonfoil': self.safe_get(card_data, 'nonfoil', False),
            'finishes': json.dumps(self.safe_get(card_data, 'finishes', [])),
            'oversized': self.safe_get(card_data, 'oversized', False),
            'promo': self.safe_get(card_data, 'promo', False),
            'reprint': self.safe_get(card_data, 'reprint', False),
            'variation': self.safe_get(card_data, 'variation', False),
            'set_id': self.safe_get(card_data, 'set_id'),
            'set_code': self.safe_get(card_data, 'set'),
            'set_name': self.safe_get(card_data, 'set_name'),
            'set_type': self.safe_get(card_data, 'set_type'),
            'collector_number': self.safe_get(card_data, 'collector_number'),
            'digital': self.safe_get(card_data, 'digital', False),
            'rarity': self.safe_get(card_data, 'rarity'),
            'card_back_id': self.safe_get(card_data, 'card_back_id'),
            'artist': self.safe_get(card_data, 'artist'),
            'artist_ids': json.dumps(self.safe_get(card_data, 'artist_ids', [])),
            'border_color': self.safe_get(card_data, 'border_color'),
            'frame': self.safe_get(card_data, 'frame'),
            'full_art': self.safe_get(card_data, 'full_art', False),
            'textless': self.safe_get(card_data, 'textless', False),
            'booster': self.safe_get(card_data, 'booster', False),
            'story_spotlight': self.safe_get(card_data, 'story_spotlight', False),
            'prices': json.dumps(self.safe_get(card_data, 'prices', {})),
            'related_uris': json.dumps(self.safe_get(card_data, 'related_uris', {})),
            'purchase_uris': json.dumps(self.safe_get(card_data, 'purchase_uris', {}))
        }
    
    def insert_card(self, card_data: Dict[str, Any]) -> bool:
        """Insert a new card using the InsertCard stored procedure."""
        if not self.connection:
            logger.error("No database connection")
            return False
        
        try:
            prepared_data = self.prepare_card_data(card_data)
            cursor = self.connection.cursor()
            
            # Call the InsertCard stored procedure
            cursor.callproc('InsertCard', [
                prepared_data['id'],
                prepared_data['oracle_id'],
                prepared_data['multiverse_ids'],
                prepared_data['mtgo_id'],
                prepared_data['arena_id'],
                prepared_data['tcgplayer_id'],
                prepared_data['cardmarket_id'],
                prepared_data['name'],
                prepared_data['lang'],
                prepared_data['released_at'],
                prepared_data['uri'],
                prepared_data['scryfall_uri'],
                prepared_data['layout'],
                prepared_data['highres_image'],
                prepared_data['image_status'],
                prepared_data['image_uris'],
                prepared_data['mana_cost'],
                prepared_data['cmc'],
                prepared_data['type_line'],
                prepared_data['oracle_text'],
                prepared_data['colors'],
                prepared_data['color_identity'],
                prepared_data['keywords'],
                prepared_data['produced_mana'],
                prepared_data['legalities'],
                prepared_data['games'],
                prepared_data['reserved'],
                prepared_data['game_changer'],
                prepared_data['foil'],
                prepared_data['nonfoil'],
                prepared_data['finishes'],
                prepared_data['oversized'],
                prepared_data['promo'],
                prepared_data['reprint'],
                prepared_data['variation'],
                prepared_data['set_id'],
                prepared_data['set_code'],
                prepared_data['set_name'],
                prepared_data['set_type'],
                prepared_data['collector_number'],
                prepared_data['digital'],
                prepared_data['rarity'],
                prepared_data['card_back_id'],
                prepared_data['artist'],
                prepared_data['artist_ids'],
                prepared_data['border_color'],
                prepared_data['frame'],
                prepared_data['full_art'],
                prepared_data['textless'],
                prepared_data['booster'],
                prepared_data['story_spotlight'],
                prepared_data['prices'],
                prepared_data['related_uris'],
                prepared_data['purchase_uris']
            ])
            
            self.connection.commit()
            cursor.close()
            logger.info(f"Successfully inserted card: {prepared_data['name']} ({prepared_data['id']})")
            return True
            
        except Error as e:
            logger.error(f"Error inserting card {card_data.get('name', 'Unknown')}: {e}")
            self.connection.rollback()
            return False
    
    def update_card(self, card_data: Dict[str, Any]) -> bool:
        """Update an existing card using the UpdateCard stored procedure."""
        if not self.connection:
            logger.error("No database connection")
            return False
        
        try:
            prepared_data = self.prepare_card_data(card_data)
            cursor = self.connection.cursor()
            
            # Call the UpdateCard stored procedure
            cursor.callproc('UpdateCard', [
                prepared_data['id'],
                prepared_data['oracle_id'],
                prepared_data['multiverse_ids'],
                prepared_data['mtgo_id'],
                prepared_data['arena_id'],
                prepared_data['tcgplayer_id'],
                prepared_data['cardmarket_id'],
                prepared_data['name'],
                prepared_data['lang'],
                prepared_data['released_at'],
                prepared_data['uri'],
                prepared_data['scryfall_uri'],
                prepared_data['layout'],
                prepared_data['highres_image'],
                prepared_data['image_status'],
                prepared_data['image_uris'],
                prepared_data['mana_cost'],
                prepared_data['cmc'],
                prepared_data['type_line'],
                prepared_data['oracle_text'],
                prepared_data['colors'],
                prepared_data['color_identity'],
                prepared_data['keywords'],
                prepared_data['produced_mana'],
                prepared_data['legalities'],
                prepared_data['games'],
                prepared_data['reserved'],
                prepared_data['game_changer'],
                prepared_data['foil'],
                prepared_data['nonfoil'],
                prepared_data['finishes'],
                prepared_data['oversized'],
                prepared_data['promo'],
                prepared_data['reprint'],
                prepared_data['variation'],
                prepared_data['set_id'],
                prepared_data['set_code'],
                prepared_data['set_name'],
                prepared_data['set_type'],
                prepared_data['collector_number'],
                prepared_data['digital'],
                prepared_data['rarity'],
                prepared_data['card_back_id'],
                prepared_data['artist'],
                prepared_data['artist_ids'],
                prepared_data['border_color'],
                prepared_data['frame'],
                prepared_data['full_art'],
                prepared_data['textless'],
                prepared_data['booster'],
                prepared_data['story_spotlight'],
                prepared_data['prices'],
                prepared_data['related_uris'],
                prepared_data['purchase_uris']
            ])
            
            self.connection.commit()
            cursor.close()
            logger.info(f"Successfully updated card: {prepared_data['name']} ({prepared_data['id']})")
            return True
            
        except Error as e:
            logger.error(f"Error updating card {card_data.get('name', 'Unknown')}: {e}")
            self.connection.rollback()
            return False
    
    def upsert_card(self, card_data: Dict[str, Any]) -> bool:
        """Insert or update a card using the UpsertCard stored procedure."""
        if not self.connection:
            logger.error("No database connection")
            return False
        
        try:
            prepared_data = self.prepare_card_data(card_data)
            cursor = self.connection.cursor()
            
            # Call the UpsertCard stored procedure
            cursor.callproc('UpsertCard', [
                prepared_data['id'],
                prepared_data['oracle_id'],
                prepared_data['multiverse_ids'],
                prepared_data['mtgo_id'],
                prepared_data['arena_id'],
                prepared_data['tcgplayer_id'],
                prepared_data['cardmarket_id'],
                prepared_data['name'],
                prepared_data['lang'],
                prepared_data['released_at'],
                prepared_data['uri'],
                prepared_data['scryfall_uri'],
                prepared_data['layout'],
                prepared_data['highres_image'],
                prepared_data['image_status'],
                prepared_data['image_uris'],
                prepared_data['mana_cost'],
                prepared_data['cmc'],
                prepared_data['type_line'],
                prepared_data['oracle_text'],
                prepared_data['colors'],
                prepared_data['color_identity'],
                prepared_data['keywords'],
                prepared_data['produced_mana'],
                prepared_data['legalities'],
                prepared_data['games'],
                prepared_data['reserved'],
                prepared_data['game_changer'],
                prepared_data['foil'],
                prepared_data['nonfoil'],
                prepared_data['finishes'],
                prepared_data['oversized'],
                prepared_data['promo'],
                prepared_data['reprint'],
                prepared_data['variation'],
                prepared_data['set_id'],
                prepared_data['set_code'],
                prepared_data['set_name'],
                prepared_data['set_type'],
                prepared_data['collector_number'],
                prepared_data['digital'],
                prepared_data['rarity'],
                prepared_data['card_back_id'],
                prepared_data['artist'],
                prepared_data['artist_ids'],
                prepared_data['border_color'],
                prepared_data['frame'],
                prepared_data['full_art'],
                prepared_data['textless'],
                prepared_data['booster'],
                prepared_data['story_spotlight'],
                prepared_data['prices'],
                prepared_data['related_uris'],
                prepared_data['purchase_uris']
            ])
            
            self.connection.commit()
            cursor.close()
            logger.info(f"Successfully upserted card: {prepared_data['name']} ({prepared_data['id']})")
            return True
            
        except Error as e:
            logger.error(f"Error upserting card {card_data.get('name', 'Unknown')}: {e}")
            self.connection.rollback()
            return False
    
    def card_exists(self, card_id: str) -> bool:
        """Check if a card exists in the database."""
        if not self.connection:
            return False
        
        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT COUNT(*) FROM cards WHERE id = %s", (card_id,))
            result = cursor.fetchone()
            cursor.close()
            return result[0] > 0
        except Error as e:
            logger.error(f"Error checking if card exists: {e}")
            return False
    
    def process_cards_batch(self, cards: List[Dict[str, Any]], batch_size: int = 100) -> Dict[str, int]:
        """Process multiple cards in batches."""
        stats = {'inserted': 0, 'updated': 0, 'failed': 0}
        total_cards = len(cards)
        
        for i in range(0, total_cards, batch_size):
            batch = cards[i:i + batch_size]
            logger.info(f"Processing batch {i//batch_size + 1}: cards {i+1}-{min(i+batch_size, total_cards)} of {total_cards}")
            
            for card in batch:
                try:
                    card_id = card.get('id')
                    if not card_id:
                        logger.warning("Card missing ID, skipping")
                        stats['failed'] += 1
                        continue
                    
                    if self.card_exists(card_id):
                        if self.update_card(card):
                            stats['updated'] += 1
                        else:
                            stats['failed'] += 1
                    else:
                        if self.insert_card(card):
                            stats['inserted'] += 1
                        else:
                            stats['failed'] += 1
                            
                except Exception as e:
                    logger.error(f"Unexpected error processing card: {e}")
                    stats['failed'] += 1
        
        return stats


class MTGCardProcessor:
    """Main application class for processing MTG card data."""
    
    def __init__(self, db_config: Dict[str, Any]):
        """Initialize with database configuration."""
        self.db = MTGCardDatabase(**db_config)
    
    def load_json_file(self, file_path: str) -> Optional[List[Dict[str, Any]]]:
        """Load JSON data from file."""
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)
                
            # Handle different JSON structures
            if isinstance(data, list):
                return data
            elif isinstance(data, dict):
                # If it's a single card object
                return [data]
            else:
                logger.error(f"Unexpected JSON structure in {file_path}")
                return None
                
        except FileNotFoundError:
            logger.error(f"File not found: {file_path}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in {file_path}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error loading {file_path}: {e}")
            return None
    
    def process_file(self, file_path: str, batch_size: int = 100) -> bool:
        """Process a JSON file containing card data."""
        logger.info(f"Loading cards from: {file_path}")
        
        cards = self.load_json_file(file_path)
        if not cards:
            return False
        
        logger.info(f"Loaded {len(cards)} cards from file")
        
        if not self.db.connect():
            return False
        
        try:
            stats = self.db.process_cards_batch(cards, batch_size)
            logger.info(f"Processing complete - Inserted: {stats['inserted']}, "
                       f"Updated: {stats['updated']}, Failed: {stats['failed']}")
            return stats['failed'] == 0
        finally:
            self.db.disconnect()
    
    def process_single_card(self, card_data: Dict[str, Any]) -> bool:
        """Process a single card object."""
        if not self.db.connect():
            return False
        
        try:
            success = self.db.upsert_card(card_data)
            return success
        finally:
            self.db.disconnect()


def create_config_template() -> Dict[str, Any]:
    """Create a template configuration dictionary."""
    return {
        'host': 'localhost',
        'database': 'mtg_cards',
        'user': 'your_username',
        'password': 'your_password',
        'port': 3306
    }


def load_config(config_path: str) -> Optional[Dict[str, Any]]:
    """Load database configuration from JSON file."""
    try:
        with open(config_path, 'r') as file:
            return json.load(file)
    except FileNotFoundError:
        logger.error(f"Config file not found: {config_path}")
        logger.info("Creating template config file...")
        with open(config_path, 'w') as file:
            json.dump(create_config_template(), file, indent=2)
        logger.info(f"Template config created at {config_path}. Please update with your database credentials.")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in config file: {e}")
        return None


def main():
    """Main application entry point."""
    parser = argparse.ArgumentParser(description='MTG Card Database Processor')
    parser.add_argument('json_file', help='Path to JSON file containing card data')
    parser.add_argument('--config', default='db_config.json', help='Database config file (default: db_config.json)')
    parser.add_argument('--batch-size', type=int, default=100, help='Batch size for processing (default: 100)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Load database configuration
    db_config = load_config(args.config)
    if not db_config:
        sys.exit(1)
    
    # Validate JSON file exists
    if not Path(args.json_file).exists():
        logger.error(f"JSON file not found: {args.json_file}")
        sys.exit(1)
    
    # Process the file
    processor = MTGCardProcessor(db_config)
    success = processor.process_file(args.json_file, args.batch_size)
    
    if success:
        logger.info("All cards processed successfully!")
        sys.exit(0)
    else:
        logger.error("Some cards failed to process. Check logs for details.")
        sys.exit(1)


if __name__ == '__main__':
    main()


# Example usage functions for testing
def example_usage():
    """Example of how to use the application programmatically."""
    
    # Database configuration
    db_config = {
        'host': 'localhost',
        'database': 'make-your-trade',
        'user': 'your_username',
        'password': 'your_password',
        'port': 3306
    }
    
    # Initialize processor
    processor = MTGCardProcessor(db_config)
    
    # Process a JSON file
    success = processor.process_file('cards.json', batch_size=50)
    if success:
        print("File processed successfully!")
    
    # Or process a single card object
    card_data = {
        "id": "0000419b-0bba-4488-8f7a-6194544ce91e",
        "name": "Forest",
        "set": "blb",
        # ... other card data
    }
    
    success = processor.process_single_card(card_data)
    if success:
        print("Single card processed successfully!")


# Requirements for pip install:
"""
Required packages:
pip install mysql-connector-python

Usage examples:

1. Command line usage:
   python mtg_cards_app.py cards.json
   python mtg_cards_app.py cards.json --config my_config.json --batch-size 50 --verbose

2. Programmatic usage:
   from mtg_cards_app import MTGCardProcessor
   
   db_config = {...}
   processor = MTGCardProcessor(db_config)
   processor.process_file('cards.json')

3. Configuration file (db_config.json):
   {
     "host": "localhost",
     "database": "mtg_cards",
     "user": "your_username", 
     "password": "your_password",
     "port": 3306
   }
"""