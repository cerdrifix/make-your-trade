#!/usr/bin/env python3
"""
Magic: The Gathering Card Data Importer - OPTIMIZED VERSION
Imports card data from Scryfall JSON into PostgreSQL database
"""

import json
import hashlib
import psycopg2
import psycopg2.extras
import requests
from datetime import datetime
from typing import Dict, Any, Optional, List, Tuple
import sys
from tqdm import tqdm
import logging
from concurrent.futures import ThreadPoolExecutor
import threading
from psycopg2 import extensions

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
        self.existing_hashes = {}  # Cache for hash lookups
        self.existing_artists = {}  # Cache for artist lookups
        self.lock = threading.Lock()  # For thread safety

    

    def ensure_clean_transaction(self):
        """Ensure connection is not left in an aborted/unknown transaction state."""
        if not self.conn:
            return
        try:
            status = self.conn.get_transaction_status()
        except Exception:
            # If we cannot get status, reconnect defensively
            try:
                self.disconnect_db()
            finally:
                self.connect_db()
            return

        # If last operation failed, the transaction is aborted until we rollback
        if status == extensions.TRANSACTION_STATUS_INERROR:
            try:
                self.conn.rollback()
            except Exception:
                # If rollback itself fails, reconnect
                try:
                    self.disconnect_db()
                finally:
                    self.connect_db()
                return

        # Unknown means libpq can't determine it (e.g., connection issues)
        elif status == extensions.TRANSACTION_STATUS_UNKNOWN:
            try:
                self.disconnect_db()
            finally:
                self.connect_db()
            return

        # If we reconnected, refresh the cursor
        if self.cursor is None or self.cursor.closed:
            self.cursor = self.conn.cursor()

        
    def connect_db(self):
        """Establish database connection"""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.cursor = self.conn.cursor()
            # Optimize connection for bulk operations
            self.conn.autocommit = False
            
            # Ensure we start with a clean transaction state
            self.conn.rollback()
            
            # Apply optimizations based on PostgreSQL version
            try:
                self.cursor.execute("SET synchronous_commit = off")
                self.cursor.execute("SET wal_buffers = 16MB")
                self.cursor.execute("SET commit_delay = 1000")
                self.cursor.execute("SET commit_siblings = 10")
                
                # For PostgreSQL 9.5+ use max_wal_size instead of checkpoint_segments
                self.cursor.execute("SHOW server_version_num")
                version_num = int(self.cursor.fetchone()[0])
                
                if version_num >= 90500:  # PostgreSQL 9.5+
                    self.cursor.execute("SET max_wal_size = 4GB")
                else:
                    self.cursor.execute("SET checkpoint_segments = 32")
                    
                logger.info("✓ Database connection established with optimizations")
            except Exception as opt_e:
                logger.warning(f"⚠️  Some optimizations failed (this is usually OK): {opt_e}")
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

    
    def get_last_update_date(self):
        self.ensure_clean_transaction()
        try:
            self.cursor.execute("SELECT MAX(completed_at) FROM import_status WHERE status = 'completed'")
            result = self.cursor.fetchone()
            return result[0] if result and result[0] else None
        except Exception as e:
            logger.error(f"✗ Failed to fetch last update date: {e}")
            return None

    
    def load_existing_hashes(self):
        """Pre-load all existing card hashes for faster comparison"""
        logger.info("📋 Loading existing card hashes...")
        self.ensure_clean_transaction()
        try:
            self.cursor.execute("SELECT id, data_hash FROM card")
            self.existing_hashes = dict(self.cursor.fetchall())
            logger.info(f"✓ Loaded {len(self.existing_hashes):,} existing card hashes")
        except Exception as e:
            logger.error(f"✗ Failed to load existing hashes: {e}")
            self.existing_hashes = {}
    
    def load_existing_artists(self):
        """Pre-load all existing artists for faster lookup"""
        logger.info("🎨 Loading existing artists...")
        self.ensure_clean_transaction()
        try:
            self.cursor.execute("SELECT name, id FROM artist")
            self.existing_artists = dict(self.cursor.fetchall())
            logger.info(f"✓ Loaded {len(self.existing_artists):,} existing artists")
        except Exception as e:
            logger.error(f"✗ Failed to load existing artists: {e}")
            self.existing_artists = {}
    
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
        """Check if card needs to be updated based on hash comparison (using cache)"""
        existing_hash = self.existing_hashes.get(card_id)
        if existing_hash is None:
            return True  # Card doesn't exist, needs insert
        return existing_hash != data_hash  # Compare hashes
    
    def get_or_create_artist_cached(self, artist_name: str) -> Optional[int]:
        """Get existing artist ID or create new artist (using cache)"""
        if not artist_name:
            return None
        
        # Check cache first
        if artist_name in self.existing_artists:
            return self.existing_artists[artist_name]
        
        # Not in cache, need to create
        try:
            self.cursor.execute(
                "INSERT INTO artist (name) VALUES (%s) RETURNING id",
                (artist_name,)
            )
            artist_id = self.cursor.fetchone()[0]
            
            # Update cache
            self.existing_artists[artist_name] = artist_id
            return artist_id
            
        except psycopg2.IntegrityError:
            # Artist was created by another process, try to fetch
            self.conn.rollback()
            self.cursor.execute(
                "SELECT id FROM artist WHERE name = %s",
                (artist_name,)
            )
            result = self.cursor.fetchone()
            if result:
                artist_id = result[0]
                self.existing_artists[artist_name] = artist_id
                return artist_id
            return None
        except Exception as e:
            logger.error(f"✗ Error handling artist '{artist_name}': {e}")
            return None
    
    def prepare_card_batch(self, cards_batch: List[Dict[str, Any]]) -> Tuple[List, List, List]:
        """Prepare batch data for bulk operations"""
        cards_to_upsert = []
        sets_to_upsert = []
        related_data = []
        
        seen_sets = set()
        
        for card_data in cards_batch:
            try:
                data_hash = self.calculate_hash(card_data)
                card_id = card_data.get('id')
                
                # Only process if card needs update
                if not self.card_needs_update(card_id, data_hash):
                    continue
                
                # Get or create artist
                artist_id = self.get_or_create_artist_cached(card_data.get('artist'))
                
                # Prepare set data (avoid duplicates in batch)
                set_id = card_data.get('set')
                if set_id and set_id not in seen_sets:
                    seen_sets.add(set_id)
                    set_data = (
                        set_id,
                        card_data.get('set_name'),
                        card_data.get('set_type'),
                        card_data.get('released_at'),
                        card_data.get('digital', False),
                        card_data.get('scryfall_set_uri'),
                        card_data.get('set_uri'),
                        card_data.get('set_search_uri')
                    )
                    sets_to_upsert.append(set_data)
                
                # Prepare card data
                card_fields = (
                    card_id,
                    card_data.get('oracle_id'),
                    json.dumps(card_data.get('multiverse_ids', [])),
                    card_data.get('mtgo_id'),
                    card_data.get('mtgo_foil_id'),
                    card_data.get('tcgplayer_id'),
                    card_data.get('cardmarket_id'),
                    card_data.get('name'),
                    card_data.get('lang'),
                    card_data.get('released_at'),
                    card_data.get('uri'),
                    card_data.get('scryfall_uri'),
                    card_data.get('layout'),
                    card_data.get('image_status'),
                    json.dumps(card_data.get('image_uris', {})),
                    card_data.get('mana_cost'),
                    card_data.get('cmc'),
                    card_data.get('type_line'),
                    card_data.get('oracle_text'),
                    card_data.get('flavor_text'),
                    card_data.get('power'),
                    card_data.get('toughness'),
                    card_data.get('loyalty'),
                    card_data.get('set'),
                    card_data.get('set_name'),
                    card_data.get('set_type'),
                    card_data.get('set_uri'),
                    card_data.get('set_search_uri'),
                    card_data.get('scryfall_set_uri'),
                    card_data.get('rulings_uri'),
                    card_data.get('prints_search_uri'),
                    card_data.get('collector_number'),
                    card_data.get('digital', False),
                    card_data.get('rarity'),
                    artist_id,
                    card_data.get('illustration_id'),
                    card_data.get('border_color'),
                    card_data.get('frame'),
                    json.dumps(card_data.get('frame_effects', [])),
                    card_data.get('security_stamp'),
                    card_data.get('full_art', False),
                    card_data.get('textless', False),
                    card_data.get('booster', False),
                    card_data.get('story_spotlight', False),
                    json.dumps(card_data.get('prices', {})),
                    json.dumps(card_data.get('purchase_uris', {})),
                    json.dumps(card_data.get('related_uris', {})),
                    data_hash
                )
                cards_to_upsert.append(card_fields)
                
                # Prepare related data
                related_data.append({
                    'card_id': card_id,
                    'colors': card_data.get('colors', []),
                    'color_identity': card_data.get('color_identity', []),
                    'type_names': card_data.get('type_names', []),
                    'subtypes': card_data.get('subtypes', []),
                    'supertypes': card_data.get('supertypes', []),
                    'legalities': card_data.get('legalities', {})
                })
                
            except Exception as e:
                logger.error(f"✗ Error preparing card data: {e}")
                continue
        
        return cards_to_upsert, sets_to_upsert, related_data
    
    def bulk_upsert_sets(self, sets_data: List):
        """Bulk upsert sets using execute_values"""
        if not sets_data:
            return
        
        try:
            psycopg2.extras.execute_values(
                self.cursor,
                """
                INSERT INTO "set" (id, name, set_type, released_at, digital, scryfall_uri, uri, search_uri)
                VALUES %s
                ON CONFLICT (id) DO UPDATE SET
                    name = EXCLUDED.name,
                    set_type = EXCLUDED.set_type,
                    released_at = EXCLUDED.released_at,
                    digital = EXCLUDED.digital,
                    scryfall_uri = EXCLUDED.scryfall_uri,
                    uri = EXCLUDED.uri,
                    search_uri = EXCLUDED.search_uri
                """,
                sets_data,
                template=None,
                page_size=1000
            )
        except Exception as e:
            logger.error(f"✗ Error bulk upserting sets: {e}")
            raise
    
    def bulk_upsert_cards(self, cards_data: List):
        """Bulk upsert cards using execute_values"""
        if not cards_data:
            return
        
        try:
            psycopg2.extras.execute_values(
                self.cursor,
                """
                INSERT INTO card (
                    id, oracle_id, multiverse_ids, mtgo_id, mtgo_foil_id, tcgplayer_id, cardmarket_id,
                    name, lang, released_at, uri, scryfall_uri, layout, image_status, image_uris,
                    mana_cost, cmc, type_line, oracle_text, flavor_text, power, toughness, loyalty,
                    set_id, set_name, set_type, set_uri, set_search_uri, scryfall_set_uri,
                    rulings_uri, prints_search_uri, collector_number, digital, rarity, artist_id,
                    illustration_id, border_color, frame, frame_effects, security_stamp,
                    full_art, textless, booster, story_spotlight, prices, purchase_uris,
                    related_uris, data_hash
                ) VALUES %s
                ON CONFLICT (id) DO UPDATE SET
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
                """,
                cards_data,
                template=None,
                page_size=500
            )
        except Exception as e:
            logger.error(f"✗ Error bulk upserting cards: {e}")
            raise
    
    def bulk_insert_related_data(self, related_data_list: List):
        """Bulk insert card-related data"""
        if not related_data_list:
            return
        
        try:
            # Collect all related data for bulk operations
            colors_data = []
            color_identity_data = []
            types_data = []
            subtypes_data = []
            supertypes_data = []
            legalities_data = []
            
            card_ids = [data['card_id'] for data in related_data_list]
            
            # First, delete existing related data for these cards
            if card_ids:
                card_ids_tuple = tuple(card_ids)
                tables = ['card_colors', 'card_color_identity', 'card_types', 'card_subtypes', 'card_supertypes', 'legality']
                for table in tables:
                    self.cursor.execute(f"DELETE FROM {table} WHERE card_id = ANY(%s)", (card_ids,))
            
            # Prepare bulk data
            for data in related_data_list:
                card_id = data['card_id']
                
                # Colors
                for color in data['colors']:
                    colors_data.append((card_id, color))
                
                # Color identity
                for color in data['color_identity']:
                    color_identity_data.append((card_id, color))
                
                # Types
                for type_name in data['type_names']:
                    types_data.append((card_id, type_name))
                
                # Subtypes
                for subtype in data['subtypes']:
                    subtypes_data.append((card_id, subtype))
                
                # Supertypes
                for supertype in data['supertypes']:
                    supertypes_data.append((card_id, supertype))
                
                # Legalities
                for format_name, legality_status in data['legalities'].items():
                    legalities_data.append((card_id, format_name, legality_status))
            
            # Bulk insert all related data
            if colors_data:
                psycopg2.extras.execute_values(
                    self.cursor,
                    "INSERT INTO card_colors (card_id, color) VALUES %s",
                    colors_data,
                    page_size=1000
                )
            
            if color_identity_data:
                psycopg2.extras.execute_values(
                    self.cursor,
                    "INSERT INTO card_color_identity (card_id, color) VALUES %s",
                    color_identity_data,
                    page_size=1000
                )
            
            if types_data:
                psycopg2.extras.execute_values(
                    self.cursor,
                    "INSERT INTO card_types (card_id, type_name) VALUES %s",
                    types_data,
                    page_size=1000
                )
            
            if subtypes_data:
                psycopg2.extras.execute_values(
                    self.cursor,
                    "INSERT INTO card_subtypes (card_id, subtype_name) VALUES %s",
                    subtypes_data,
                    page_size=1000
                )
            
            if supertypes_data:
                psycopg2.extras.execute_values(
                    self.cursor,
                    "INSERT INTO card_supertypes (card_id, supertype_name) VALUES %s",
                    supertypes_data,
                    page_size=1000
                )
            
            if legalities_data:
                psycopg2.extras.execute_values(
                    self.cursor,
                    "INSERT INTO legality (card_id, format_name, legality_status) VALUES %s",
                    legalities_data,
                    page_size=1000
                )
                
        except Exception as e:
            logger.error(f"✗ Error bulk inserting related data: {e}")
            raise
    
    def process_batch(self, cards_batch: List[Dict[str, Any]]) -> Tuple[int, int]:
        """Process a batch of cards"""
        try:
            # Ensure clean transaction state before processing
            self.ensure_clean_transaction()
            
            # Prepare batch data
            cards_data, sets_data, related_data = self.prepare_card_batch(cards_batch)
            
            if not cards_data:
                return 0, len(cards_batch)  # updated, skipped
            
            # Bulk operations
            self.bulk_upsert_sets(sets_data)
            self.bulk_upsert_cards(cards_data)
            self.bulk_insert_related_data(related_data)
            
            updated = len(cards_data)
            skipped = len(cards_batch) - updated
            
            return updated, skipped
            
        except Exception as e:
            logger.error(f"✗ Error processing batch: {e}")
            self.ensure_clean_transaction()
            raise
    
    def download_and_import(self, url: str, batch_size: int = 1000):
        """Download JSON data and import to database using batch processing"""
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
            
            # Load existing data for faster comparisons
            self.load_existing_hashes()
            self.load_existing_artists()
            
            # Start import status tracking
            self.import_status_id = self.start_import_status(total_cards)
            
            # Process cards in batches
            processed = 0
            updated_total = 0
            skipped_total = 0
            errors = 0
            
            with tqdm(total=total_cards, desc="Importing cards", unit="cards") as pbar:
                for i in range(0, total_cards, batch_size):
                    try:
                        batch = cards_data[i:i + batch_size]
                        updated, skipped = self.process_batch(batch)
                        
                        updated_total += updated
                        skipped_total += skipped
                        processed += len(batch)
                        
                        # Commit after each batch
                        self.conn.commit()
                        self.update_import_status(processed)
                        
                        pbar.set_postfix(
                            updated=updated_total, 
                            skipped=skipped_total, 
                            errors=errors,
                            batch_size=len(batch)
                        )
                        pbar.update(len(batch))
                        
                    except Exception as e:
                        errors += 1
                        logger.error(f"✗ Error processing batch {i//batch_size + 1}: {e}")
                        self.conn.rollback()  # Rollback failed batch
                        pbar.update(min(batch_size, total_cards - i))
                        continue
            
            # Final status update
            self.update_import_status(processed, 'completed')
            
            # Print summary
            logger.info(f"\n✅ Import completed successfully!")
            logger.info(f"📊 Summary:")
            logger.info(f"   Total cards processed: {processed:,}")
            logger.info(f"   Cards updated/inserted: {updated_total:,}")
            logger.info(f"   Cards skipped (no changes): {skipped_total:,}")
            logger.info(f"   Batch errors: {errors:,}")
            
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
    # data_url = 'https://data.scryfall.io/default-cards/default-cards-20250830211634.json' # Default data set
    # data_url = 'https://data.scryfall.io/all-cards/all-cards-20250904213604.json'  # Full data set
    last_update_date = datetime.min  # Default to very old date if no previous update found
    
    # Create importer and run
    importer = MTGImporter(db_config)
    
    try:
        importer.connect_db()

        # Check last update date
        last_update = importer.get_last_update_date()
        if last_update:
            logger.info(f"🕒 Last card update in database: {last_update}")
            last_update_date = last_update.replace(tzinfo=None)  # Ensure naive datetime for comparison


        # 2. Call Scryfall bulk-data API
        resp = requests.get("https://api.scryfall.com/bulk-data")
        resp.raise_for_status()
        bulk_data = resp.json()

        # 3. Find the "All Cards" bulk data entry
        all_cards_entry = next((entry for entry in bulk_data['data'] if entry['type'] == 'all_cards'), None)
        if not all_cards_entry:
            logger.error("✗ 'All Cards' bulk data entry not found")
            sys.exit(1)

        # 4. Compare updated_at timestamps
        scryfall_updated_at = datetime.fromisoformat(all_cards_entry['updated_at'].replace('Z', '+00:00')).replace(tzinfo=None)
        if last_update and scryfall_updated_at <= last_update_date:
            logger.info("✅ Database is already up-to-date with Scryfall data")
            importer.update_import_status(1, 'completed', 'Update not needed; data is current')
            sys.exit(0)

        data_url = all_cards_entry['download_uri']


        # Process in batches of 1000 cards (adjust as needed)
        importer.download_and_import(data_url, batch_size=1000)
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