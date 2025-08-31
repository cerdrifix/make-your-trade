import requests
import json
import logging
import hashlib
from datetime import datetime
from app import app, db
from models import Card, Set, Artist, Legality, ImportStatus
from sqlalchemy.exc import IntegrityError

logger = logging.getLogger(__name__)


class DataImporter:

    def __init__(self):
        self.batch_size = 1000
        # self.scryfall_url = "https://data.scryfall.io/all-cards/all-cards-20250830092116.json"
        self.scryfall_url = "https://data.scryfall.io/default-cards/default-cards-20250830211634.json"
    
    def _calculate_hash(self, data_dict, keys_to_hash):
        """Calculate SHA-256 hash from selected keys in data dictionary."""
        # Sort keys for consistent hash calculation
        hash_data = {}
        for key in sorted(keys_to_hash):
            if key in data_dict and data_dict[key] is not None:
                hash_data[key] = data_dict[key]
        
        # Convert to JSON string with sorted keys for consistency
        hash_string = json.dumps(hash_data, sort_keys=True, default=str)
        return hashlib.sha256(hash_string.encode('utf-8')).hexdigest()
    
    def _calculate_card_hash(self, card_data):
        """Calculate hash for card data."""
        card_keys = [
            'id', 'name', 'oracle_text', 'mana_cost', 'cmc', 'type_line',
            'power', 'toughness', 'loyalty', 'rarity', 'artist', 'flavor_text',
            'colors', 'color_identity', 'legalities', 'prices'
        ]
        return self._calculate_hash(card_data, card_keys)
    
    def _calculate_set_hash(self, card_data):
        """Calculate hash for set data."""
        set_keys = [
            'set', 'set_name', 'set_type', 'released_at', 'digital'
        ]
        return self._calculate_hash(card_data, set_keys)
    
    def _calculate_artist_hash(self, artist_name):
        """Calculate hash for artist data."""
        if not artist_name:
            return None
        return hashlib.sha256(artist_name.encode('utf-8')).hexdigest()
    
    def _calculate_legalities_hash(self, legalities_data):
        """Calculate hash for card legalities."""
        if not legalities_data:
            return None
        hash_string = json.dumps(legalities_data, sort_keys=True)
        return hashlib.sha256(hash_string.encode('utf-8')).hexdigest()
    
    def update_hashes(self, import_status_id):
        """Update hashes for all existing records in the database."""
        with app.app_context():
            import_record = ImportStatus.query.get(import_status_id)
            if not import_record:
                logger.error(f"Import status record not found: {import_status_id}")
                return
            
            try:
                # Update status to running
                import_record.status = 'running'
                db.session.commit()
                
                logger.info("Starting hash update for all existing records...")
                
                # Get all cards from the database
                all_cards = Card.query.all()
                total_cards = len(all_cards)
                import_record.total_cards = total_cards
                db.session.commit()
                
                logger.info(f"Updating hashes for {total_cards} cards...")
                
                # Process cards in batches for hash updates
                processed = 0
                for i in range(0, total_cards, self.batch_size):
                    batch = all_cards[i:i + self.batch_size]
                    
                    for card in batch:
                        # Create a mock card_data dict to calculate hash
                        card_data = {
                            'id': card.id,
                            'name': card.name,
                            'oracle_text': card.oracle_text,
                            'mana_cost': card.mana_cost,
                            'cmc': card.cmc,
                            'type_line': card.type_line,
                            'power': card.power,
                            'toughness': card.toughness,
                            'loyalty': card.loyalty,
                            'rarity': card.rarity,
                            'artist': card.artist_rel.name if card.artist_rel else None,
                            'flavor_text': card.flavor_text,
                            'colors': None,  # Would need to reconstruct from association table
                            'color_identity': None,  # Would need to reconstruct from association table
                            'legalities': {leg.format_name: leg.legality_status for leg in card.legalities},
                            'prices': card.prices
                        }
                        
                        # Calculate and update card hash
                        new_hash = self._calculate_card_hash(card_data)
                        card.data_hash = new_hash
                        
                        # Update set hash if set exists
                        if card.set_rel:
                            set_data = {
                                'set': card.set_rel.id,
                                'set_name': card.set_rel.name,
                                'set_type': card.set_rel.set_type,
                                'released_at': card.set_rel.released_at,
                                'digital': card.set_rel.digital
                            }
                            set_hash = self._calculate_set_hash(set_data)
                            card.set_rel.data_hash = set_hash
                        
                        # Update artist hash if artist exists
                        if card.artist_rel:
                            artist_hash = self._calculate_artist_hash(card.artist_rel.name)
                            card.artist_rel.data_hash = artist_hash
                        
                        # Update legalities hashes
                        legalities_data = {leg.format_name: leg.legality_status for leg in card.legalities}
                        if legalities_data:
                            legalities_hash = self._calculate_legalities_hash(legalities_data)
                            for legality in card.legalities:
                                legality.data_hash = legalities_hash
                    
                    # Commit the batch
                    db.session.commit()
                    processed += len(batch)
                    import_record.processed_cards = processed
                    db.session.commit()
                    
                    logger.info(f"Updated hashes for {processed}/{total_cards} cards ({(processed/total_cards)*100:.1f}%)")
                
                # Mark as completed
                import_record.status = 'completed'
                import_record.completed_at = datetime.utcnow()
                db.session.commit()
                
                logger.info("Hash update completed successfully!")
                
            except Exception as e:
                logger.error(f"Hash update failed: {str(e)}")
                import_record.status = 'failed'
                import_record.error_message = str(e)
                import_record.completed_at = datetime.utcnow()
                db.session.commit()

    def import_data(self, import_status_id):
        """Main import function that runs in background thread."""
        with app.app_context():
            import_record = ImportStatus.query.get(import_status_id)
            if not import_record:
                logger.error(
                    f"Import status record not found: {import_status_id}")
                return

            try:
                # Update status to running
                import_record.status = 'running'
                db.session.commit()

                logger.info(
                    "Starting Magic: The Gathering card data import...")

                # Download and process the data
                self._download_and_process_data(import_record)

                # Mark as completed
                import_record.status = 'completed'
                import_record.completed_at = datetime.utcnow()
                db.session.commit()

                logger.info("Import completed successfully!")

            except Exception as e:
                logger.error(f"Import failed: {str(e)}")
                import_record.status = 'failed'
                import_record.error_message = str(e)
                import_record.completed_at = datetime.utcnow()
                db.session.commit()

    def _download_and_process_data(self, import_record):
        """Download JSON data from Scryfall and process it."""
        logger.info(f"Downloading data from {self.scryfall_url}")

        try:
            response = requests.get(self.scryfall_url, stream=True)
            response.raise_for_status()

            # Parse JSON data
            data = response.json()

            if not isinstance(data, list):
                raise ValueError("Expected JSON array of cards")

            total_cards = len(data)
            import_record.total_cards = total_cards
            db.session.commit()

            logger.info(f"Processing {total_cards} cards...")

            # Process cards in batches
            processed = 0
            for i in range(0, total_cards, self.batch_size):
                batch = data[i:i + self.batch_size]
                self._process_batch(batch, import_record)

                processed += len(batch)
                import_record.processed_cards = processed
                db.session.commit()

                logger.info(
                    f"Processed {processed}/{total_cards} cards ({(processed/total_cards)*100:.1f}%)"
                )

        except requests.RequestException as e:
            raise Exception(f"Failed to download data: {str(e)}")
        except json.JSONDecodeError as e:
            raise Exception(f"Failed to parse JSON data: {str(e)}")

    def _process_batch(self, cards_batch, import_record):
        """Process a batch of cards."""
        try:
            for card_data in cards_batch:
                self._process_card(card_data)

            # Commit the batch
            db.session.commit()

        except Exception as e:
            db.session.rollback()
            logger.error(f"Error processing batch: {str(e)}")
            raise

    def _process_card(self, card_data):
        """Process a single card and related entities."""
        try:
            # Process set first
            set_obj = self._get_or_create_set(card_data)

            # Process artist
            artist_obj = self._get_or_create_artist(card_data)

            # Create or update card
            card = self._create_or_update_card(card_data, set_obj, artist_obj)

            # Process legalities
            self._process_legalities(card, card_data)

        except Exception as e:
            logger.error(
                f"Error processing card {card_data.get('name', 'Unknown')}: {str(e)}"
            )
            # Continue processing other cards

    def _get_or_create_set(self, card_data):
        """Get or create a Set object with hash comparison."""
        set_code = card_data.get('set')
        if not set_code:
            return None

        # Calculate hash for set data
        set_hash = self._calculate_set_hash(card_data)
        
        set_obj = Set.query.filter_by(id=set_code).first()
        if not set_obj:
            # Create new set
            set_obj = Set(
                id=set_code,
                name=card_data.get('set_name', ''),
                set_type=card_data.get('set_type'),
                released_at=card_data.get('released_at'),
                digital=card_data.get('digital', False),
                scryfall_uri=card_data.get('scryfall_set_uri'),
                uri=card_data.get('set_uri'),
                search_uri=card_data.get('set_search_uri'),
                data_hash=set_hash
            )
            db.session.add(set_obj)
        elif set_obj.data_hash != set_hash:
            # Update existing set if hash changed
            set_obj.name = card_data.get('set_name', set_obj.name)
            set_obj.set_type = card_data.get('set_type') or set_obj.set_type
            set_obj.released_at = card_data.get('released_at') or set_obj.released_at
            set_obj.digital = card_data.get('digital', set_obj.digital)
            set_obj.scryfall_uri = card_data.get('scryfall_set_uri') or set_obj.scryfall_uri
            set_obj.uri = card_data.get('set_uri') or set_obj.uri
            set_obj.search_uri = card_data.get('set_search_uri') or set_obj.search_uri
            set_obj.data_hash = set_hash

        return set_obj

    def _get_or_create_artist(self, card_data):
        """Get or create an Artist object with hash comparison."""
        artist_name = card_data.get('artist')
        if not artist_name:
            return None

        # Calculate hash for artist data
        artist_hash = self._calculate_artist_hash(artist_name)
        
        artist = Artist.query.filter_by(name=artist_name).first()
        if not artist:
            # Create new artist
            artist = Artist(name=artist_name, data_hash=artist_hash)
            db.session.add(artist)
        elif artist.data_hash != artist_hash:
            # Update hash if changed (though artist name rarely changes)
            artist.data_hash = artist_hash

        return artist

    def _create_or_update_card(self, card_data, set_obj, artist_obj):
        """Create or update a Card object with hash comparison."""
        card_id = card_data.get('id')
        if not card_id:
            logger.warning("Card without ID found, skipping")
            return None

        # Calculate hash for card data
        card_hash = self._calculate_card_hash(card_data)
        
        # Check if card already exists
        card = Card.query.filter_by(id=card_id).first()
        if card:
            # Only update if hash has changed
            if card.data_hash != card_hash:
                self._update_card_data(card, card_data, set_obj, artist_obj, card_hash)
                logger.debug(f"Updated card {card.name} (hash changed)")
            else:
                logger.debug(f"Skipped card {card.name} (no changes)")
        else:
            # Create new card
            card = self._create_new_card(card_data, set_obj, artist_obj, card_hash)
            db.session.add(card)
            logger.debug(f"Created new card {card.name}")

        return card

    def _create_new_card(self, card_data, set_obj, artist_obj, card_hash):
        """Create a new Card object."""
        card = Card(
            id=card_data.get('id'),
            oracle_id=card_data.get('oracle_id'),
            multiverse_ids=json.dumps(card_data.get('multiverse_ids', [])),
            mtgo_id=card_data.get('mtgo_id'),
            mtgo_foil_id=card_data.get('mtgo_foil_id'),
            tcgplayer_id=card_data.get('tcgplayer_id'),
            cardmarket_id=card_data.get('cardmarket_id'),
            name=card_data.get('name', ''),
            lang=card_data.get('lang', 'en'),
            released_at=card_data.get('released_at'),
            uri=card_data.get('uri'),
            scryfall_uri=card_data.get('scryfall_uri'),
            layout=card_data.get('layout'),
            image_status=card_data.get('image_status'),
            image_uris=json.dumps(card_data.get('image_uris', {})),
            mana_cost=card_data.get('mana_cost'),
            cmc=card_data.get('cmc'),
            type_line=card_data.get('type_line'),
            oracle_text=card_data.get('oracle_text'),
            flavor_text=card_data.get('flavor_text'),
            power=card_data.get('power'),
            toughness=card_data.get('toughness'),
            loyalty=card_data.get('loyalty'),
            set_id=set_obj.id if set_obj else None,
            set_name=card_data.get('set_name'),
            set_type=card_data.get('set_type'),
            set_uri=card_data.get('set_uri'),
            set_search_uri=card_data.get('set_search_uri'),
            scryfall_set_uri=card_data.get('scryfall_set_uri'),
            rulings_uri=card_data.get('rulings_uri'),
            prints_search_uri=card_data.get('prints_search_uri'),
            collector_number=card_data.get('collector_number'),
            digital=card_data.get('digital', False),
            rarity=card_data.get('rarity'),
            artist_id=artist_obj.id if artist_obj else None,
            illustration_id=card_data.get('illustration_id'),
            border_color=card_data.get('border_color'),
            frame=card_data.get('frame'),
            frame_effects=json.dumps(card_data.get('frame_effects', [])),
            security_stamp=card_data.get('security_stamp'),
            full_art=card_data.get('full_art', False),
            textless=card_data.get('textless', False),
            booster=card_data.get('booster', False),
            story_spotlight=card_data.get('story_spotlight', False),
            prices=json.dumps(card_data.get('prices', {})),
            purchase_uris=json.dumps(card_data.get('purchase_uris', {})),
            related_uris=json.dumps(card_data.get('related_uris', {})),
            data_hash=card_hash
        )

        return card

    def _update_card_data(self, card, card_data, set_obj, artist_obj, card_hash):
        """Update existing card with new data."""
        # Update basic card information
        card.name = card_data.get('name', card.name)
        card.oracle_text = card_data.get('oracle_text', card.oracle_text)
        card.flavor_text = card_data.get('flavor_text', card.flavor_text)
        card.mana_cost = card_data.get('mana_cost', card.mana_cost)
        card.cmc = card_data.get('cmc', card.cmc)
        card.type_line = card_data.get('type_line', card.type_line)
        card.power = card_data.get('power', card.power)
        card.toughness = card_data.get('toughness', card.toughness)
        card.loyalty = card_data.get('loyalty', card.loyalty)
        card.rarity = card_data.get('rarity', card.rarity)
        card.prices = json.dumps(card_data.get('prices', {}))
        card.data_hash = card_hash

        # Update relationships
        if set_obj:
            card.set_id = set_obj.id
        if artist_obj:
            card.artist_id = artist_obj.id

    def _process_legalities(self, card, card_data):
        """Process card legalities with hash comparison."""
        if not card:
            return

        legalities_data = card_data.get('legalities', {})
        if not legalities_data:
            return

        # Calculate hash for legalities
        legalities_hash = self._calculate_legalities_hash(legalities_data)
        
        # Check if any existing legalities have different hash
        existing_legalities = Legality.query.filter_by(card_id=card.id).all()
        needs_update = True
        
        if existing_legalities:
            # Check if any existing legality has a different hash
            existing_hash = existing_legalities[0].data_hash if existing_legalities else None
            if existing_hash == legalities_hash:
                needs_update = False
        
        if needs_update:
            # Remove existing legalities for this card
            Legality.query.filter_by(card_id=card.id).delete()

            # Add new legalities
            for format_name, legality_status in legalities_data.items():
                legality = Legality(
                    card_id=card.id,
                    format_name=format_name,
                    legality_status=legality_status,
                    data_hash=legalities_hash
                )
                db.session.add(legality)
