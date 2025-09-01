#!/usr/bin/env python3
"""
Magic: The Gathering Card Data Importer
Imports card data from Scryfall JSON into PostgreSQL database
"""

import json
import hashlib
import psycopg2
import requests
from datetime import datetime
from typing import Dict, Any, Optional, List
import sys
from tqdm import tqdm
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MTGImporter:
    def __init__(self, db_config: Dict[str, str]):
        """Initialize the importer with database configuration"""
        self.db_config = db_config
        self.conn = None
        self.cursor = None
        self.import_status_id = None
        
    def connect_db(self):
        """Establish database connection"""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.cursor = self.conn.cursor()
            logger.info("✓ Database connection established")
        except Exception as e:
            logger.error(f"✗ Failed to connect to database: {e}")
            sys.exit(1)
    
    def disconnect_db(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("✓ Database connection closed")
    
    def calculate_hash(self, card_data: Dict[str, Any]) -> str:
        """Calculate SHA-256 hash of card data for change detection"""
        # Create a consistent string representation of the card data
        card_str = json.dumps(card_data, sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(card_str.encode('utf-8')).hexdigest()
    
    def start_import_status(self, total_cards: int) -> int:
        """Create import status record and return its ID"""
        try:
            self.cursor.execute("""
                INSERT INTO import_status (started_at, status, total_cards, processed_cards)
                VALUES (%s, %s, %s, %s)
                RETURNING id
            """, (datetime.now(), 'running', total_cards, 0))
            
            import_status_id = self.cursor.fetchone()[0]
            self.conn.commit()
            return import_status_id
        except Exception as e:
            logger.error(f"✗ Failed to create import status: {e}")
            return None
    
    def update_import_status(self, processed_cards: int, status: str = 'running', error_message: str = None):
        """Update import status record"""
        if not self.import_status_id:
            return
            
        try:
            if status == 'completed':
                self.cursor.execute("""
                    UPDATE import_status 
                    SET processed_cards = %s, status = %s, completed_at = %s, error_message = %s
                    WHERE id = %s
                """, (processed_cards, status, datetime.now(), error_message, self.import_status_id))
            else:
                self.cursor.execute("""
                    UPDATE import_status 
                    SET processed_cards = %s, status = %s, error_message = %s
                    WHERE id = %s
                """, (processed_cards, status, error_message, self.import_status_id))
            
            self.conn.commit()
        except Exception as e:
            logger.error(f"✗ Failed to update import status: {e}")
    
    def card_needs_update(self, card_id: str, data_hash: str) -> bool:
        """Check if card needs to be updated based on hash comparison"""
        try:
            self.cursor.execute(
                "SELECT data_hash FROM card WHERE id = %s",
                (card_id,)
            )
            result = self.cursor.fetchone()
            
            if result is None:
                return True  # Card doesn't exist, needs insert
            
            return result[0] != data_hash  # Compare hashes
            
        except Exception as e:
            logger.error(f"✗ Error checking card hash for {card_id}: {e}")
            return True  # Assume needs update on error
    
    def get_or_create_artist(self, artist_name: str) -> Optional[int]:
        """Get existing artist ID or create new artist"""
        if not artist_name:
            return None
            
        try:
            # Try to get existing artist
            self.cursor.execute(
                "SELECT id FROM artist WHERE name = %s",
                (artist_name,)
            )
            result = self.cursor.fetchone()
            
            if result:
                return result[0]
            
            # Create new artist
            self.cursor.execute(
                "INSERT INTO artist (name) VALUES (%s) RETURNING id",
                (artist_name,)
            )
            return self.cursor.fetchone()[0]
            
        except Exception as e:
            logger.error(f"✗ Error handling artist '{artist_name}': {e}")
            return None
    
    def insert_or_update_set(self, card_data: Dict[str, Any]):
        """Insert or update set information"""
        try:
            set_data = {
                'id': card_data.get('set'),
                'name': card_data.get('set_name'),
                'set_type': card_data.get('set_type'),
                'released_at': card_data.get('released_at'),
                'digital': card_data.get('digital', False),
                'scryfall_uri': card_data.get('scryfall_set_uri'),
                'uri': card_data.get('set_uri'),
                'search_uri': card_data.get('set_search_uri')
            }
            
            # Use UPSERT (INSERT ... ON CONFLICT)
            self.cursor.execute("""
                INSERT INTO "set" (id, name, set_type, released_at, digital, scryfall_uri, uri, search_uri)
                VALUES (%(id)s, %(name)s, %(set_type)s, %(released_at)s, %(digital)s, %(scryfall_uri)s, %(uri)s, %(search_uri)s)
                ON CONFLICT (id) DO UPDATE SET
                    name = EXCLUDED.name,
                    set_type = EXCLUDED.set_type,
                    released_at = EXCLUDED.released_at,
                    digital = EXCLUDED.digital,
                    scryfall_uri = EXCLUDED.scryfall_uri,
                    uri = EXCLUDED.uri,
                    search_uri = EXCLUDED.search_uri
            """, set_data)
            
        except Exception as e:
            logger.error(f"✗ Error inserting/updating set: {e}")
    
    def insert_card(self, card_data: Dict[str, Any], data_hash: str):
        """Insert or update card data"""
        try:
            # Get or create artist
            artist_id = self.get_or_create_artist(card_data.get('artist'))
            
            # Insert/update set
            self.insert_or_update_set(card_data)
            
            # Prepare card data
            card_fields = {
                'id': card_data.get('id'),
                'oracle_id': card_data.get('oracle_id'),
                'multiverse_ids': json.dumps(card_data.get('multiverse_ids', [])),
                'mtgo_id': card_data.get('mtgo_id'),
                'mtgo_foil_id': card_data.get('mtgo_foil_id'),
                'tcgplayer_id': card_data.get('tcgplayer_id'),
                'cardmarket_id': card_data.get('cardmarket_id'),
                'name': card_data.get('name'),
                'lang': card_data.get('lang'),
                'released_at': card_data.get('released_at'),
                'uri': card_data.get('uri'),
                'scryfall_uri': card_data.get('scryfall_uri'),
                'layout': card_data.get('layout'),
                'image_status': card_data.get('image_status'),
                'image_uris': json.dumps(card_data.get('image_uris', {})),
                'mana_cost': card_data.get('mana_cost'),
                'cmc': card_data.get('cmc'),
                'type_line': card_data.get('type_line'),
                'oracle_text': card_data.get('oracle_text'),
                'flavor_text': card_data.get('flavor_text'),
                'power': card_data.get('power'),
                'toughness': card_data.get('toughness'),
                'loyalty': card_data.get('loyalty'),
                'set_id': card_data.get('set'),
                'set_name': card_data.get('set_name'),
                'set_type': card_data.get('set_type'),
                'set_uri': card_data.get('set_uri'),
                'set_search_uri': card_data.get('set_search_uri'),
                'scryfall_set_uri': card_data.get('scryfall_set_uri'),
                'rulings_uri': card_data.get('rulings_uri'),
                'prints_search_uri': card_data.get('prints_search_uri'),
                'collector_number': card_data.get('collector_number'),
                'digital': card_data.get('digital', False),
                'rarity': card_data.get('rarity'),
                'artist_id': artist_id,
                'illustration_id': card_data.get('illustration_id'),
                'border_color': card_data.get('border_color'),
                'frame': card_data.get('frame'),
                'frame_effects': json.dumps(card_data.get('frame_effects', [])),
                'security_stamp': card_data.get('security_stamp'),
                'full_art': card_data.get('full_art', False),
                'textless': card_data.get('textless', False),
                'booster': card_data.get('booster', False),
                'story_spotlight': card_data.get('story_spotlight', False),
                'prices': json.dumps(card_data.get('prices', {})),
                'purchase_uris': json.dumps(card_data.get('purchase_uris', {})),
                'related_uris': json.dumps(card_data.get('related_uris', {})),
                'data_hash': data_hash
            }
            
            # Insert/update card
            self.cursor.execute("""
                INSERT INTO card (
                    id, oracle_id, multiverse_ids, mtgo_id, mtgo_foil_id, tcgplayer_id, cardmarket_id,
                    name, lang, released_at, uri, scryfall_uri, layout, image_status, image_uris,
                    mana_cost, cmc, type_line, oracle_text, flavor_text, power, toughness, loyalty,
                    set_id, set_name, set_type, set_uri, set_search_uri, scryfall_set_uri,
                    rulings_uri, prints_search_uri, collector_number, digital, rarity, artist_id,
                    illustration_id, border_color, frame, frame_effects, security_stamp,
                    full_art, textless, booster, story_spotlight, prices, purchase_uris,
                    related_uris, data_hash
                ) VALUES (
                    %(id)s, %(oracle_id)s, %(multiverse_ids)s, %(mtgo_id)s, %(mtgo_foil_id)s,
                    %(tcgplayer_id)s, %(cardmarket_id)s, %(name)s, %(lang)s, %(released_at)s,
                    %(uri)s, %(scryfall_uri)s, %(layout)s, %(image_status)s, %(image_uris)s,
                    %(mana_cost)s, %(cmc)s, %(type_line)s, %(oracle_text)s, %(flavor_text)s,
                    %(power)s, %(toughness)s, %(loyalty)s, %(set_id)s, %(set_name)s, %(set_type)s,
                    %(set_uri)s, %(set_search_uri)s, %(scryfall_set_uri)s, %(rulings_uri)s,
                    %(prints_search_uri)s, %(collector_number)s, %(digital)s, %(rarity)s,
                    %(artist_id)s, %(illustration_id)s, %(border_color)s, %(frame)s,
                    %(frame_effects)s, %(security_stamp)s, %(full_art)s, %(textless)s,
                    %(booster)s, %(story_spotlight)s, %(prices)s, %(purchase_uris)s,
                    %(related_uris)s, %(data_hash)s
                ) ON CONFLICT (id) DO UPDATE SET
                    oracle_id = EXCLUDED.oracle_id,
                    multiverse_ids = EXCLUDED.multiverse_ids,
                    mtgo_id = EXCLUDED.mtgo_id,
                    mtgo_foil_id = EXCLUDED.mtgo_foil_id,
                    tcgplayer_id = EXCLUDED.tcgplayer_id,
                    cardmarket_id = EXCLUDED.cardmarket_id,
                    name = EXCLUDED.name,
                    lang = EXCLUDED.lang,
                    released_at = EXCLUDED.released_at,
                    uri = EXCLUDED.uri,
                    scryfall_uri = EXCLUDED.scryfall_uri,
                    layout = EXCLUDED.layout,
                    image_status = EXCLUDED.image_status,
                    image_uris = EXCLUDED.image_uris,
                    mana_cost = EXCLUDED.mana_cost,
                    cmc = EXCLUDED.cmc,
                    type_line = EXCLUDED.type_line,
                    oracle_text = EXCLUDED.oracle_text,
                    flavor_text = EXCLUDED.flavor_text,
                    power = EXCLUDED.power,
                    toughness = EXCLUDED.toughness,
                    loyalty = EXCLUDED.loyalty,
                    set_id = EXCLUDED.set_id,
                    set_name = EXCLUDED.set_name,
                    set_type = EXCLUDED.set_type,
                    set_uri = EXCLUDED.set_uri,
                    set_search_uri = EXCLUDED.set_search_uri,
                    scryfall_set_uri = EXCLUDED.scryfall_set_uri,
                    rulings_uri = EXCLUDED.rulings_uri,
                    prints_search_uri = EXCLUDED.prints_search_uri,
                    collector_number = EXCLUDED.collector_number,
                    digital = EXCLUDED.digital,
                    rarity = EXCLUDED.rarity,
                    artist_id = EXCLUDED.artist_id,
                    illustration_id = EXCLUDED.illustration_id,
                    border_color = EXCLUDED.border_color,
                    frame = EXCLUDED.frame,
                    frame_effects = EXCLUDED.frame_effects,
                    security_stamp = EXCLUDED.security_stamp,
                    full_art = EXCLUDED.full_art,
                    textless = EXCLUDED.textless,
                    booster = EXCLUDED.booster,
                    story_spotlight = EXCLUDED.story_spotlight,
                    prices = EXCLUDED.prices,
                    purchase_uris = EXCLUDED.purchase_uris,
                    related_uris = EXCLUDED.related_uris,
                    data_hash = EXCLUDED.data_hash
            """, card_fields)
            
            # Handle related data (colors, types, legalities, etc.)
            self.insert_card_related_data(card_data)
            
        except Exception as e:
            logger.error(f"✗ Error inserting card '{card_data.get('name', 'Unknown')}': {e}")
            raise
    
    def insert_card_related_data(self, card_data: Dict[str, Any]):
        """Insert card colors, types, subtypes, supertypes, and legalities"""
        card_id = card_data.get('id')
        
        try:
            # Clear existing related data
            tables = ['card_colors', 'card_color_identity', 'card_types', 'card_subtypes', 'card_supertypes', 'legality']
            for table in tables:
                self.cursor.execute(f"DELETE FROM {table} WHERE card_id = %s", (card_id,))
            
            # Insert colors
            for color in card_data.get('colors', []):
                self.cursor.execute(
                    "INSERT INTO card_colors (card_id, color) VALUES (%s, %s)",
                    (card_id, color)
                )
            
            # Insert color identity
            for color in card_data.get('color_identity', []):
                self.cursor.execute(
                    "INSERT INTO card_color_identity (card_id, color) VALUES (%s, %s)",
                    (card_id, color)
                )
            
            # Insert types, subtypes, supertypes
            for type_name in card_data.get('type_names', []):
                self.cursor.execute(
                    "INSERT INTO card_types (card_id, type_name) VALUES (%s, %s)",
                    (card_id, type_name)
                )
            
            for subtype in card_data.get('subtypes', []):
                self.cursor.execute(
                    "INSERT INTO card_subtypes (card_id, subtype_name) VALUES (%s, %s)",
                    (card_id, subtype)
                )
            
            for supertype in card_data.get('supertypes', []):
                self.cursor.execute(
                    "INSERT INTO card_supertypes (card_id, supertype_name) VALUES (%s, %s)",
                    (card_id, supertype)
                )
            
            # Insert legalities
            legalities = card_data.get('legalities', {})
            for format_name, legality_status in legalities.items():
                self.cursor.execute(
                    "INSERT INTO legality (card_id, format_name, legality_status) VALUES (%s, %s, %s)",
                    (card_id, format_name, legality_status)
                )
                
        except Exception as e:
            logger.error(f"✗ Error inserting related data for card {card_id}: {e}")
            raise
    
    def download_and_import(self, url: str):
        """Download JSON data and import to database"""
        logger.info(f"🌐 Downloading data from: {url}")
        
        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()
            
            # Get total file size for progress
            total_size = int(response.headers.get('content-length', 0))
            logger.info(f"📦 File size: {total_size / 1024 / 1024:.2f} MB")
            
            # Download and parse JSON
            logger.info("📥 Downloading and parsing JSON data...")
            content = response.content.decode('utf-8')
            cards_data = json.loads(content)
            
            total_cards = len(cards_data)
            logger.info(f"🎴 Found {total_cards:,} cards to process")
            
            # Start import status tracking
            self.import_status_id = self.start_import_status(total_cards)
            
            # Process cards with progress bar
            processed = 0
            updated = 0
            skipped = 0
            errors = 0
            
            with tqdm(total=total_cards, desc="Importing cards", unit="cards") as pbar:
                for card_data in cards_data:
                    try:
                        card_id = card_data.get('id')
                        data_hash = self.calculate_hash(card_data)
                        
                        if self.card_needs_update(card_id, data_hash):
                            self.insert_card(card_data, data_hash)
                            updated += 1
                            pbar.set_postfix(updated=updated, skipped=skipped, errors=errors)
                        else:
                            skipped += 1
                            pbar.set_postfix(updated=updated, skipped=skipped, errors=errors)
                        
                        processed += 1
                        
                        # Commit every 100 cards
                        if processed % 100 == 0:
                            self.conn.commit()
                            self.update_import_status(processed)
                        
                        pbar.update(1)
                        
                    except Exception as e:
                        errors += 1
                        logger.error(f"✗ Error processing card: {e}")
                        pbar.set_postfix(updated=updated, skipped=skipped, errors=errors)
                        pbar.update(1)
                        continue
            
            # Final commit
            self.conn.commit()
            self.update_import_status(processed, 'completed')
            
            # Print summary
            logger.info(f"\n✅ Import completed successfully!")
            logger.info(f"📊 Summary:")
            logger.info(f"   Total cards processed: {processed:,}")
            logger.info(f"   Cards updated/inserted: {updated:,}")
            logger.info(f"   Cards skipped (no changes): {skipped:,}")
            logger.info(f"   Errors encountered: {errors:,}")
            
        except Exception as e:
            error_msg = f"Import failed: {e}"
            logger.error(f"✗ {error_msg}")
            if hasattr(self, 'import_status_id') and self.import_status_id:
                self.update_import_status(0, 'failed', error_msg)
            raise


def main():
    """Main function"""
    # Database configuration - modify these values for your setup
    db_config = {
        'host': 'localhost',
        'database': 'myt',
        'user': 'cerdrifix',
        'password': '',
        'port': 5432
    }
    
    # Data URL
    data_url = 'https://data.scryfall.io/default-cards/default-cards-20250830211634.json'
    
    # Create importer and run
    importer = MTGImporter(db_config)
    
    try:
        importer.connect_db()
        importer.download_and_import(data_url)
    except KeyboardInterrupt:
        logger.info("\n⚠️  Import interrupted by user")
        if importer.import_status_id:
            importer.update_import_status(0, 'cancelled', 'Import cancelled by user')
    except Exception as e:
        logger.error(f"✗ Import failed: {e}")
        sys.exit(1)
    finally:
        importer.disconnect_db()


if __name__ == "__main__":
    main()