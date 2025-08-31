from app import db
from sqlalchemy import Text, DateTime, Boolean, Integer, String, ForeignKey, Table
from sqlalchemy.orm import relationship
from datetime import datetime

# Association tables for many-to-many relationships
card_colors = Table('card_colors', db.metadata,
    db.Column('card_id', String, ForeignKey('card.id'), primary_key=True),
    db.Column('color', String(1), primary_key=True)
)

card_color_identity = Table('card_color_identity', db.metadata,
    db.Column('card_id', String, ForeignKey('card.id'), primary_key=True),
    db.Column('color', String(1), primary_key=True)
)

card_types = Table('card_types', db.metadata,
    db.Column('card_id', String, ForeignKey('card.id'), primary_key=True),
    db.Column('type_name', String, primary_key=True)
)

card_subtypes = Table('card_subtypes', db.metadata,
    db.Column('card_id', String, ForeignKey('card.id'), primary_key=True),
    db.Column('subtype_name', String, primary_key=True)
)

card_supertypes = Table('card_supertypes', db.metadata,
    db.Column('card_id', String, ForeignKey('card.id'), primary_key=True),
    db.Column('supertype_name', String, primary_key=True)
)

class Set(db.Model):
    __tablename__ = 'set'
    
    id = db.Column(String, primary_key=True)  # set code
    name = db.Column(String, nullable=False)
    set_type = db.Column(String)
    released_at = db.Column(String)
    block_code = db.Column(String)
    block = db.Column(String)
    parent_set_code = db.Column(String)
    card_count = db.Column(Integer)
    digital = db.Column(Boolean, default=False)
    foil_only = db.Column(Boolean, default=False)
    nonfoil_only = db.Column(Boolean, default=False)
    scryfall_uri = db.Column(String)
    uri = db.Column(String)
    icon_svg_uri = db.Column(String)
    search_uri = db.Column(String)
    data_hash = db.Column(String(64))  # SHA-256 hash of set data
    
    # Relationship to cards
    cards = relationship("Card", back_populates="set_rel")

class Artist(db.Model):
    __tablename__ = 'artist'
    
    id = db.Column(Integer, primary_key=True, autoincrement=True)
    name = db.Column(String, unique=True, nullable=False)
    data_hash = db.Column(String(64))  # SHA-256 hash of artist data
    
    # Relationship to cards
    cards = relationship("Card", back_populates="artist_rel")

class Card(db.Model):
    __tablename__ = 'card'
    
    id = db.Column(String, primary_key=True)  # scryfall id
    oracle_id = db.Column(String)
    multiverse_ids = db.Column(Text)  # JSON array as string
    mtgo_id = db.Column(Integer)
    mtgo_foil_id = db.Column(Integer)
    tcgplayer_id = db.Column(Integer)
    cardmarket_id = db.Column(Integer)
    
    # Card identity
    name = db.Column(String, nullable=False)
    lang = db.Column(String, default='en')
    released_at = db.Column(String)
    uri = db.Column(String)
    scryfall_uri = db.Column(String)
    layout = db.Column(String)
    
    # Images
    image_status = db.Column(String)
    image_uris = db.Column(Text)  # JSON as string
    
    # Mana cost and CMC
    mana_cost = db.Column(String)
    cmc = db.Column(db.Float)
    
    # Type line
    type_line = db.Column(String)
    
    # Card text
    oracle_text = db.Column(Text)
    flavor_text = db.Column(Text)
    
    # Power and toughness
    power = db.Column(String)
    toughness = db.Column(String)
    loyalty = db.Column(String)
    
    # Set information
    set_id = db.Column(String, ForeignKey('set.id'))
    set_name = db.Column(String)
    set_type = db.Column(String)
    set_uri = db.Column(String)
    set_search_uri = db.Column(String)
    scryfall_set_uri = db.Column(String)
    rulings_uri = db.Column(String)
    prints_search_uri = db.Column(String)
    collector_number = db.Column(String)
    digital = db.Column(Boolean, default=False)
    rarity = db.Column(String)
    
    # Artist information
    artist_id = db.Column(Integer, ForeignKey('artist.id'))
    illustration_id = db.Column(String)
    border_color = db.Column(String)
    frame = db.Column(String)
    frame_effects = db.Column(Text)  # JSON array as string
    security_stamp = db.Column(String)
    full_art = db.Column(Boolean, default=False)
    textless = db.Column(Boolean, default=False)
    booster = db.Column(Boolean, default=False)
    story_spotlight = db.Column(Boolean, default=False)
    
    # Prices
    prices = db.Column(Text)  # JSON as string
    
    # Purchase URIs
    purchase_uris = db.Column(Text)  # JSON as string
    
    # Related URIs
    related_uris = db.Column(Text)  # JSON as string
    data_hash = db.Column(String(64))  # SHA-256 hash of card data
    
    # Relationships
    set_rel = relationship("Set", back_populates="cards")
    artist_rel = relationship("Artist", back_populates="cards")
    legalities = relationship("Legality", back_populates="card", cascade="all, delete-orphan")
    
    # Many-to-many relationships
    colors = relationship("Card", secondary=card_colors, 
                         primaryjoin="Card.id == card_colors.c.card_id",
                         secondaryjoin="card_colors.c.color == Card.id",
                         viewonly=True)
    
    def __repr__(self):
        return f'<Card {self.name}>'

class Legality(db.Model):
    __tablename__ = 'legality'
    
    id = db.Column(Integer, primary_key=True, autoincrement=True)
    card_id = db.Column(String, ForeignKey('card.id'), nullable=False)
    format_name = db.Column(String, nullable=False)
    legality_status = db.Column(String, nullable=False)
    data_hash = db.Column(String(64))  # SHA-256 hash of legality data
    
    # Relationship
    card = relationship("Card", back_populates="legalities")

class ImportStatus(db.Model):
    __tablename__ = 'import_status'
    
    id = db.Column(Integer, primary_key=True, autoincrement=True)
    started_at = db.Column(DateTime, default=datetime.utcnow)
    completed_at = db.Column(DateTime)
    status = db.Column(String, default='pending')  # pending, running, completed, failed
    total_cards = db.Column(Integer, default=0)
    processed_cards = db.Column(Integer, default=0)
    error_message = db.Column(Text)
    
    def progress_percentage(self):
        if self.total_cards == 0:
            return 0
        return round((self.processed_cards / self.total_cards) * 100, 1)
