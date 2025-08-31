-- Active: 1756582007119@@127.0.0.1@3306@mysql
-- use mysql;

-- CREATE DATABASE if not exists `make-your-trade`;

use make-your-trade;

    CREATE TABLE if not exists `users` (
      `id` int NOT NULL AUTO_INCREMENT,
      `username` varchar(50) NOT NULL,
      `email` varchar(100) NOT NULL,
      `password_hash` varchar(255) NOT NULL,
      `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (`id`),
      UNIQUE KEY `username_unique` (`username`),
      UNIQUE KEY `email_unique` (`email`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
-- MTG Cards Database Schema and Stored Procedures
-- Based on Scryfall API card object specifications

-- Create the main cards table
    CREATE TABLE cards (
        id VARCHAR(36) PRIMARY KEY,
        oracle_id VARCHAR(36),
        name VARCHAR(255) NOT NULL,
        lang VARCHAR(10),
        released_at DATE,
        uri TEXT,
        scryfall_uri TEXT,
        layout VARCHAR(50),
        highres_image BOOLEAN DEFAULT FALSE,
        image_status VARCHAR(50),
        mana_cost VARCHAR(255),
        cmc DECIMAL(10,2),
        type_line VARCHAR(255),
        oracle_text TEXT,
        reserved BOOLEAN DEFAULT FALSE,
        game_changer BOOLEAN DEFAULT FALSE,
        foil BOOLEAN DEFAULT FALSE,
        nonfoil BOOLEAN DEFAULT FALSE,
        oversized BOOLEAN DEFAULT FALSE,
        promo BOOLEAN DEFAULT FALSE,
        reprint BOOLEAN DEFAULT FALSE,
        variation BOOLEAN DEFAULT FALSE,
        set_id VARCHAR(36),
        set_code VARCHAR(10),
        set_name VARCHAR(255),
        set_type VARCHAR(50),
        collector_number VARCHAR(20),
        digital BOOLEAN DEFAULT FALSE,
        rarity VARCHAR(20),
        card_back_id VARCHAR(36),
        artist VARCHAR(255),
        border_color VARCHAR(20),
        frame VARCHAR(20),
        full_art BOOLEAN DEFAULT FALSE,
        textless BOOLEAN DEFAULT FALSE,
        booster BOOLEAN DEFAULT FALSE,
        story_spotlight BOOLEAN DEFAULT FALSE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    );

-- Create indexes for better performance
CREATE INDEX idx_cards_oracle_id ON cards(oracle_id);
CREATE INDEX idx_cards_name ON cards(name);
CREATE INDEX idx_cards_set_code ON cards(set_code);
CREATE INDEX idx_cards_released_at ON cards(released_at);

-- Table for multiverse IDs (array field)
    CREATE TABLE if not exists card_multiverse_ids (
        id INT AUTO_INCREMENT PRIMARY KEY,
        card_id VARCHAR(36),
        multiverse_id INT,
        FOREIGN KEY (card_id) REFERENCES cards(id) ON DELETE CASCADE
    );


-- Table for image URIs (nested object)
    CREATE TABLE if not exists card_image_uris (
        id INT AUTO_INCREMENT PRIMARY KEY,
        card_id VARCHAR(36),
        small_uri TEXT,
        normal_uri TEXT,
        large_uri TEXT,
        png_uri TEXT,
        art_crop_uri TEXT,
        border_crop_uri TEXT,
        FOREIGN KEY (card_id) REFERENCES cards(id) ON DELETE CASCADE
    );


-- Table for colors (array field)
    CREATE TABLE if not exists card_colors (
        id INT AUTO_INCREMENT PRIMARY KEY,
        card_id VARCHAR(36),
        color VARCHAR(10),
        FOREIGN KEY (card_id) REFERENCES cards(id) ON DELETE CASCADE
    );


-- Table for color identity (array field)
    CREATE TABLE if not exists card_color_identity (
        id INT AUTO_INCREMENT PRIMARY KEY,
        card_id VARCHAR(36),
        color VARCHAR(10),
        FOREIGN KEY (card_id) REFERENCES cards(id) ON DELETE CASCADE
    );


-- Table for keywords (array field)
    CREATE TABLE if not exists card_keywords (
        id INT AUTO_INCREMENT PRIMARY KEY,
        card_id VARCHAR(36),
        keyword VARCHAR(255),
        FOREIGN KEY (card_id) REFERENCES cards(id) ON DELETE CASCADE
    );


-- Table for produced mana (array field)
    CREATE TABLE if not exists card_produced_mana (
        id INT AUTO_INCREMENT PRIMARY KEY,
        card_id VARCHAR(36),
        mana_symbol VARCHAR(10),
        FOREIGN KEY (card_id) REFERENCES cards(id) ON DELETE CASCADE
    );

-- Table for legalities (nested object)
    CREATE TABLE if not exists card_legalities (
        id INT AUTO_INCREMENT PRIMARY KEY,
        card_id VARCHAR(36),
        format_name VARCHAR(50),
        legality VARCHAR(20),
        FOREIGN KEY (card_id) REFERENCES cards(id) ON DELETE CASCADE
    );


-- Table for games (array field)
    CREATE TABLE if not exists card_games (
        id INT AUTO_INCREMENT PRIMARY KEY,
        card_id VARCHAR(36),
        game VARCHAR(20),
        FOREIGN KEY (card_id) REFERENCES cards(id) ON DELETE CASCADE
    );


-- Table for finishes (array field)
    CREATE TABLE if not exists card_finishes (
        id INT AUTO_INCREMENT PRIMARY KEY,
        card_id VARCHAR(36),
        finish VARCHAR(20),
        FOREIGN KEY (card_id) REFERENCES cards(id) ON DELETE CASCADE
    );


-- Table for prices (nested object)
    CREATE TABLE if not exists card_prices (
        id INT AUTO_INCREMENT PRIMARY KEY,
        card_id VARCHAR(36),
        usd DECIMAL(10,2),
        usd_foil DECIMAL(10,2),
        usd_etched DECIMAL(10,2),
        eur DECIMAL(10,2),
        eur_foil DECIMAL(10,2),
        tix DECIMAL(10,2),
        FOREIGN KEY (card_id) REFERENCES cards(id) ON DELETE CASCADE
    );


-- Table for related URIs (nested object)
    CREATE TABLE if not exists card_related_uris (
        id INT AUTO_INCREMENT PRIMARY KEY,
        card_id VARCHAR(36),
        gatherer TEXT,
        tcgplayer_infinite_articles TEXT,
        tcgplayer_infinite_decks TEXT,
        edhrec TEXT,
        FOREIGN KEY (card_id) REFERENCES cards(id) ON DELETE CASCADE
    );


-- Table for purchase URIs (nested object)
    CREATE TABLE if not exists card_purchase_uris (
        id INT AUTO_INCREMENT PRIMARY KEY,
        card_id VARCHAR(36),
        tcgplayer TEXT,
        cardmarket TEXT,
        cardhoarder TEXT,
        FOREIGN KEY (card_id) REFERENCES cards(id) ON DELETE CASCADE
    );


-- Table for artist IDs (array field)
    CREATE TABLE if not exists card_artist_ids (
        id INT AUTO_INCREMENT PRIMARY KEY,
        card_id VARCHAR(36),
        artist_id VARCHAR(36),
        FOREIGN KEY (card_id) REFERENCES cards(id) ON DELETE CASCADE
    );

DELIMITER //

-- Stored procedure to insert a new card
CREATE PROCEDURE if not exists InsertCard(
    IN p_id VARCHAR(36),
    IN p_oracle_id VARCHAR(36),
    IN p_multiverse_ids JSON,
    IN p_mtgo_id INT,
    IN p_arena_id INT,
    IN p_tcgplayer_id INT,
    IN p_cardmarket_id INT,
    IN p_name VARCHAR(255),
    IN p_lang VARCHAR(10),
    IN p_released_at DATE,
    IN p_uri TEXT,
    IN p_scryfall_uri TEXT,
    IN p_layout VARCHAR(50),
    IN p_highres_image BOOLEAN,
    IN p_image_status VARCHAR(50),
    IN p_image_uris JSON,
    IN p_mana_cost VARCHAR(255),
    IN p_cmc DECIMAL(10,2),
    IN p_type_line VARCHAR(255),
    IN p_oracle_text TEXT,
    IN p_colors JSON,
    IN p_color_identity JSON,
    IN p_keywords JSON,
    IN p_produced_mana JSON,
    IN p_legalities JSON,
    IN p_games JSON,
    IN p_reserved BOOLEAN,
    IN p_game_changer BOOLEAN,
    IN p_foil BOOLEAN,
    IN p_nonfoil BOOLEAN,
    IN p_finishes JSON,
    IN p_oversized BOOLEAN,
    IN p_promo BOOLEAN,
    IN p_reprint BOOLEAN,
    IN p_variation BOOLEAN,
    IN p_set_id VARCHAR(36),
    IN p_set_code VARCHAR(10),
    IN p_set_name VARCHAR(255),
    IN p_set_type VARCHAR(50),
    IN p_collector_number VARCHAR(20),
    IN p_digital BOOLEAN,
    IN p_rarity VARCHAR(20),
    IN p_card_back_id VARCHAR(36),
    IN p_artist VARCHAR(255),
    IN p_artist_ids JSON,
    IN p_border_color VARCHAR(20),
    IN p_frame VARCHAR(20),
    IN p_full_art BOOLEAN,
    IN p_textless BOOLEAN,
    IN p_booster BOOLEAN,
    IN p_story_spotlight BOOLEAN,
    IN p_prices JSON,
    IN p_related_uris JSON,
    IN p_purchase_uris JSON
)
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        RESIGNAL;
    END;

    START TRANSACTION;

    -- Insert main card record
    INSERT INTO cards (
        id, oracle_id, name, lang, released_at, uri, scryfall_uri, layout,
        highres_image, image_status, mana_cost, cmc, type_line, oracle_text,
        reserved, game_changer, foil, nonfoil, oversized, promo, reprint, variation,
        set_id, set_code, set_name, set_type, collector_number, digital, rarity,
        card_back_id, artist, border_color, frame, full_art, textless, booster, story_spotlight
    ) VALUES (
        p_id, p_oracle_id, p_name, p_lang, p_released_at, p_uri, p_scryfall_uri, p_layout,
        p_highres_image, p_image_status, p_mana_cost, p_cmc, p_type_line, p_oracle_text,
        p_reserved, p_game_changer, p_foil, p_nonfoil, p_oversized, p_promo, p_reprint, p_variation,
        p_set_id, p_set_code, p_set_name, p_set_type, p_collector_number, p_digital, p_rarity,
        p_card_back_id, p_artist, p_border_color, p_frame, p_full_art, p_textless, p_booster, p_story_spotlight
    );

    -- Insert multiverse IDs
    IF p_multiverse_ids IS NOT NULL THEN
        INSERT INTO card_multiverse_ids (card_id, multiverse_id)
        SELECT p_id, JSON_UNQUOTE(JSON_EXTRACT(p_multiverse_ids, CONCAT('$[', numbers.n, ']')))
        FROM (SELECT 0 n UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) numbers
        WHERE JSON_EXTRACT(p_multiverse_ids, CONCAT('$[', numbers.n, ']')) IS NOT NULL;
    END IF;

    -- Insert image URIs
    IF p_image_uris IS NOT NULL THEN
        INSERT INTO card_image_uris (card_id, small_uri, normal_uri, large_uri, png_uri, art_crop_uri, border_crop_uri)
        VALUES (
            p_id,
            JSON_UNQUOTE(JSON_EXTRACT(p_image_uris, '$.small')),
            JSON_UNQUOTE(JSON_EXTRACT(p_image_uris, '$.normal')),
            JSON_UNQUOTE(JSON_EXTRACT(p_image_uris, '$.large')),
            JSON_UNQUOTE(JSON_EXTRACT(p_image_uris, '$.png')),
            JSON_UNQUOTE(JSON_EXTRACT(p_image_uris, '$.art_crop')),
            JSON_UNQUOTE(JSON_EXTRACT(p_image_uris, '$.border_crop'))
        );
    END IF;

    -- Insert colors
    IF p_colors IS NOT NULL THEN
        INSERT INTO card_colors (card_id, color)
        SELECT p_id, JSON_UNQUOTE(JSON_EXTRACT(p_colors, CONCAT('$[', numbers.n, ']')))
        FROM (SELECT 0 n UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4) numbers
        WHERE JSON_EXTRACT(p_colors, CONCAT('$[', numbers.n, ']')) IS NOT NULL;
    END IF;

    -- Insert color identity
    IF p_color_identity IS NOT NULL THEN
        INSERT INTO card_color_identity (card_id, color)
        SELECT p_id, JSON_UNQUOTE(JSON_EXTRACT(p_color_identity, CONCAT('$[', numbers.n, ']')))
        FROM (SELECT 0 n UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4) numbers
        WHERE JSON_EXTRACT(p_color_identity, CONCAT('$[', numbers.n, ']')) IS NOT NULL;
    END IF;

    -- Insert keywords
    IF p_keywords IS NOT NULL THEN
        INSERT INTO card_keywords (card_id, keyword)
        SELECT p_id, JSON_UNQUOTE(JSON_EXTRACT(p_keywords, CONCAT('$[', numbers.n, ']')))
        FROM (SELECT 0 n UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) numbers
        WHERE JSON_EXTRACT(p_keywords, CONCAT('$[', numbers.n, ']')) IS NOT NULL;
    END IF;

    -- Insert produced mana
    IF p_produced_mana IS NOT NULL THEN
        INSERT INTO card_produced_mana (card_id, mana_symbol)
        SELECT p_id, JSON_UNQUOTE(JSON_EXTRACT(p_produced_mana, CONCAT('$[', numbers.n, ']')))
        FROM (SELECT 0 n UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4) numbers
        WHERE JSON_EXTRACT(p_produced_mana, CONCAT('$[', numbers.n, ']')) IS NOT NULL;
    END IF;

    -- Insert legalities (same as insert procedure)
    IF p_legalities IS NOT NULL THEN
        INSERT INTO card_legalities (card_id, format_name, legality)
        SELECT p_id, 'standard', JSON_UNQUOTE(JSON_EXTRACT(p_legalities, '$.standard'))
        WHERE JSON_EXTRACT(p_legalities, '$.standard') IS NOT NULL
        UNION ALL
        SELECT p_id, 'future', JSON_UNQUOTE(JSON_EXTRACT(p_legalities, '$.future'))
        WHERE JSON_EXTRACT(p_legalities, '$.future') IS NOT NULL
        UNION ALL
        SELECT p_id, 'historic', JSON_UNQUOTE(JSON_EXTRACT(p_legalities, '$.historic'))
        WHERE JSON_EXTRACT(p_legalities, '$.historic') IS NOT NULL
        UNION ALL
        SELECT p_id, 'timeless', JSON_UNQUOTE(JSON_EXTRACT(p_legalities, '$.timeless'))
        WHERE JSON_EXTRACT(p_legalities, '$.timeless') IS NOT NULL
        UNION ALL
        SELECT p_id, 'gladiator', JSON_UNQUOTE(JSON_EXTRACT(p_legalities, '$.gladiator'))
        WHERE JSON_EXTRACT(p_legalities, '$.gladiator') IS NOT NULL
        UNION ALL
        SELECT p_id, 'pioneer', JSON_UNQUOTE(JSON_EXTRACT(p_legalities, '$.pioneer'))
        WHERE JSON_EXTRACT(p_legalities, '$.pioneer') IS NOT NULL
        UNION ALL
        SELECT p_id, 'modern', JSON_UNQUOTE(JSON_EXTRACT(p_legalities, '$.modern'))
        WHERE JSON_EXTRACT(p_legalities, '$.modern') IS NOT NULL
        UNION ALL
        SELECT p_id, 'legacy', JSON_UNQUOTE(JSON_EXTRACT(p_legalities, '$.legacy'))
        WHERE JSON_EXTRACT(p_legalities, '$.legacy') IS NOT NULL
        UNION ALL
        SELECT p_id, 'pauper', JSON_UNQUOTE(JSON_EXTRACT(p_legalities, '$.pauper'))
        WHERE JSON_EXTRACT(p_legalities, '$.pauper') IS NOT NULL
        UNION ALL
        SELECT p_id, 'vintage', JSON_UNQUOTE(JSON_EXTRACT(p_legalities, '$.vintage'))
        WHERE JSON_EXTRACT(p_legalities, '$.vintage') IS NOT NULL
        UNION ALL
        SELECT p_id, 'penny', JSON_UNQUOTE(JSON_EXTRACT(p_legalities, '$.penny'))
        WHERE JSON_EXTRACT(p_legalities, '$.penny') IS NOT NULL
        UNION ALL
        SELECT p_id, 'commander', JSON_UNQUOTE(JSON_EXTRACT(p_legalities, '$.commander'))
        WHERE JSON_EXTRACT(p_legalities, '$.commander') IS NOT NULL
        UNION ALL
        SELECT p_id, 'oathbreaker', JSON_UNQUOTE(JSON_EXTRACT(p_legalities, '$.oathbreaker'))
        WHERE JSON_EXTRACT(p_legalities, '$.oathbreaker') IS NOT NULL
        UNION ALL
        SELECT p_id, 'standardbrawl', JSON_UNQUOTE(JSON_EXTRACT(p_legalities, '$.standardbrawl'))
        WHERE JSON_EXTRACT(p_legalities, '$.standardbrawl') IS NOT NULL
        UNION ALL
        SELECT p_id, 'brawl', JSON_UNQUOTE(JSON_EXTRACT(p_legalities, '$.brawl'))
        WHERE JSON_EXTRACT(p_legalities, '$.brawl') IS NOT NULL
        UNION ALL
        SELECT p_id, 'alchemy', JSON_UNQUOTE(JSON_EXTRACT(p_legalities, '$.alchemy'))
        WHERE JSON_EXTRACT(p_legalities, '$.alchemy') IS NOT NULL
        UNION ALL
        SELECT p_id, 'paupercommander', JSON_UNQUOTE(JSON_EXTRACT(p_legalities, '$.paupercommander'))
        WHERE JSON_EXTRACT(p_legalities, '$.paupercommander') IS NOT NULL
        UNION ALL
        SELECT p_id, 'duel', JSON_UNQUOTE(JSON_EXTRACT(p_legalities, '$.duel'))
        WHERE JSON_EXTRACT(p_legalities, '$.duel') IS NOT NULL
        UNION ALL
        SELECT p_id, 'oldschool', JSON_UNQUOTE(JSON_EXTRACT(p_legalities, '$.oldschool'))
        WHERE JSON_EXTRACT(p_legalities, '$.oldschool') IS NOT NULL
        UNION ALL
        SELECT p_id, 'premodern', JSON_UNQUOTE(JSON_EXTRACT(p_legalities, '$.premodern'))
        WHERE JSON_EXTRACT(p_legalities, '$.premodern') IS NOT NULL
        UNION ALL
        SELECT p_id, 'predh', JSON_UNQUOTE(JSON_EXTRACT(p_legalities, '$.predh'))
        WHERE JSON_EXTRACT(p_legalities, '$.predh') IS NOT NULL;
    END IF;

    -- Insert games
    IF p_games IS NOT NULL THEN
        INSERT INTO card_games (card_id, game)
        SELECT p_id, JSON_UNQUOTE(JSON_EXTRACT(p_games, CONCAT('$[', numbers.n, ']')))
        FROM (SELECT 0 n UNION SELECT 1 UNION SELECT 2 UNION SELECT 3) numbers
        WHERE JSON_EXTRACT(p_games, CONCAT('$[', numbers.n, ']')) IS NOT NULL;
    END IF;

    -- Insert finishes
    IF p_finishes IS NOT NULL THEN
        INSERT INTO card_finishes (card_id, finish)
        SELECT p_id, JSON_UNQUOTE(JSON_EXTRACT(p_finishes, CONCAT('$[', numbers.n, ']')))
        FROM (SELECT 0 n UNION SELECT 1 UNION SELECT 2) numbers
        WHERE JSON_EXTRACT(p_finishes, CONCAT('$[', numbers.n, ']')) IS NOT NULL;
    END IF;

    -- Insert artist IDs
    IF p_artist_ids IS NOT NULL THEN
        INSERT INTO card_artist_ids (card_id, artist_id)
        SELECT p_id, JSON_UNQUOTE(JSON_EXTRACT(p_artist_ids, CONCAT('$[', numbers.n, ']')))
        FROM (SELECT 0 n UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4) numbers
        WHERE JSON_EXTRACT(p_artist_ids, CONCAT('$[', numbers.n, ']')) IS NOT NULL;
    END IF;

    -- Update/Insert prices
    DELETE FROM card_prices WHERE card_id = p_id;
    IF p_prices IS NOT NULL THEN
        INSERT INTO card_prices (card_id, usd, usd_foil, usd_etched, eur, eur_foil, tix)
        VALUES (
            p_id,
            CASE WHEN JSON_UNQUOTE(JSON_EXTRACT(p_prices, '$.usd')) = 'null' THEN NULL 
                 ELSE CAST(JSON_UNQUOTE(JSON_EXTRACT(p_prices, '$.usd')) AS DECIMAL(10,2)) END,
            CASE WHEN JSON_UNQUOTE(JSON_EXTRACT(p_prices, '$.usd_foil')) = 'null' THEN NULL 
                 ELSE CAST(JSON_UNQUOTE(JSON_EXTRACT(p_prices, '$.usd_foil')) AS DECIMAL(10,2)) END,
            CASE WHEN JSON_UNQUOTE(JSON_EXTRACT(p_prices, '$.usd_etched')) = 'null' THEN NULL 
                 ELSE CAST(JSON_UNQUOTE(JSON_EXTRACT(p_prices, '$.usd_etched')) AS DECIMAL(10,2)) END,
            CASE WHEN JSON_UNQUOTE(JSON_EXTRACT(p_prices, '$.eur')) = 'null' THEN NULL 
                 ELSE CAST(JSON_UNQUOTE(JSON_EXTRACT(p_prices, '$.eur')) AS DECIMAL(10,2)) END,
            CASE WHEN JSON_UNQUOTE(JSON_EXTRACT(p_prices, '$.eur_foil')) = 'null' THEN NULL 
                 ELSE CAST(JSON_UNQUOTE(JSON_EXTRACT(p_prices, '$.eur_foil')) AS DECIMAL(10,2)) END,
            CASE WHEN JSON_UNQUOTE(JSON_EXTRACT(p_prices, '$.tix')) = 'null' THEN NULL 
                 ELSE CAST(JSON_UNQUOTE(JSON_EXTRACT(p_prices, '$.tix')) AS DECIMAL(10,2)) END
        );
    END IF;

    -- Update/Insert related URIs
    DELETE FROM card_related_uris WHERE card_id = p_id;
    IF p_related_uris IS NOT NULL THEN
        INSERT INTO card_related_uris (card_id, gatherer, tcgplayer_infinite_articles, tcgplayer_infinite_decks, edhrec)
        VALUES (
            p_id,
            JSON_UNQUOTE(JSON_EXTRACT(p_related_uris, '$.gatherer')),
            JSON_UNQUOTE(JSON_EXTRACT(p_related_uris, '$.tcgplayer_infinite_articles')),
            JSON_UNQUOTE(JSON_EXTRACT(p_related_uris, '$.tcgplayer_infinite_decks')),
            JSON_UNQUOTE(JSON_EXTRACT(p_related_uris, '$.edhrec'))
        );
    END IF;

    -- Update/Insert purchase URIs
    DELETE FROM card_purchase_uris WHERE card_id = p_id;
    IF p_purchase_uris IS NOT NULL THEN
        INSERT INTO card_purchase_uris (card_id, tcgplayer, cardmarket, cardhoarder)
        VALUES (
            p_id,
            JSON_UNQUOTE(JSON_EXTRACT(p_purchase_uris, '$.tcgplayer')),
            JSON_UNQUOTE(JSON_EXTRACT(p_purchase_uris, '$.cardmarket')),
            JSON_UNQUOTE(JSON_EXTRACT(p_purchase_uris, '$.cardhoarder'))
        );
    END IF;

    COMMIT;
END //
DELIMITER ;

DELIMITER //

-- Stored procedure to update an existing card
CREATE PROCEDURE UpdateCard(
    IN p_id VARCHAR(36),
    IN p_oracle_id VARCHAR(36),
    IN p_multiverse_ids JSON,
    IN p_mtgo_id INT,
    IN p_arena_id INT,
    IN p_tcgplayer_id INT,
    IN p_cardmarket_id INT,
    IN p_name VARCHAR(255),
    IN p_lang VARCHAR(10),
    IN p_released_at DATE,
    IN p_uri TEXT,
    IN p_scryfall_uri TEXT,
    IN p_layout VARCHAR(50),
    IN p_highres_image BOOLEAN,
    IN p_image_status VARCHAR(50),
    IN p_image_uris JSON,
    IN p_mana_cost VARCHAR(255),
    IN p_cmc DECIMAL(10,2),
    IN p_type_line VARCHAR(255),
    IN p_oracle_text TEXT,
    IN p_colors JSON,
    IN p_color_identity JSON,
    IN p_keywords JSON,
    IN p_produced_mana JSON,
    IN p_legalities JSON,
    IN p_games JSON,
    IN p_reserved BOOLEAN,
    IN p_game_changer BOOLEAN,
    IN p_foil BOOLEAN,
    IN p_nonfoil BOOLEAN,
    IN p_finishes JSON,
    IN p_oversized BOOLEAN,
    IN p_promo BOOLEAN,
    IN p_reprint BOOLEAN,
    IN p_variation BOOLEAN,
    IN p_set_id VARCHAR(36),
    IN p_set_code VARCHAR(10),
    IN p_set_name VARCHAR(255),
    IN p_set_type VARCHAR(50),
    IN p_collector_number VARCHAR(20),
    IN p_digital BOOLEAN,
    IN p_rarity VARCHAR(20),
    IN p_card_back_id VARCHAR(36),
    IN p_artist VARCHAR(255),
    IN p_artist_ids JSON,
    IN p_border_color VARCHAR(20),
    IN p_frame VARCHAR(20),
    IN p_full_art BOOLEAN,
    IN p_textless BOOLEAN,
    IN p_booster BOOLEAN,
    IN p_story_spotlight BOOLEAN,
    IN p_prices JSON,
    IN p_related_uris JSON,
    IN p_purchase_uris JSON
)
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        RESIGNAL;
    END;

    START TRANSACTION;

    -- Update main card record
    UPDATE cards SET
        oracle_id = p_oracle_id,
        name = p_name,
        lang = p_lang,
        released_at = p_released_at,
        uri = p_uri,
        scryfall_uri = p_scryfall_uri,
        layout = p_layout,
        highres_image = p_highres_image,
        image_status = p_image_status,
        mana_cost = p_mana_cost,
        cmc = p_cmc,
        type_line = p_type_line,
        oracle_text = p_oracle_text,
        reserved = p_reserved,
        game_changer = p_game_changer,
        foil = p_foil,
        nonfoil = p_nonfoil,
        oversized = p_oversized,
        promo = p_promo,
        reprint = p_reprint,
        variation = p_variation,
        set_id = p_set_id,
        set_code = p_set_code,
        set_name = p_set_name,
        set_type = p_set_type,
        collector_number = p_collector_number,
        digital = p_digital,
        rarity = p_rarity,
        card_back_id = p_card_back_id,
        artist = p_artist,
        border_color = p_border_color,
        frame = p_frame,
        full_art = p_full_art,
        textless = p_textless,
        booster = p_booster,
        story_spotlight = p_story_spotlight,
        updated_at = CURRENT_TIMESTAMP
    WHERE id = p_id;

    -- Clear existing related data
    DELETE FROM card_multiverse_ids WHERE card_id = p_id;
    DELETE FROM card_image_uris WHERE card_id = p_id;
    DELETE FROM card_colors WHERE card_id = p_id;
    DELETE FROM card_color_identity WHERE card_id = p_id;
    DELETE FROM card_keywords WHERE card_id = p_id;
    DELETE FROM card_produced_mana WHERE card_id = p_id;
    DELETE FROM card_legalities WHERE card_id = p_id;
    DELETE FROM card_games WHERE card_id = p_id;
    DELETE FROM card_finishes WHERE card_id = p_id;
    DELETE FROM card_artist_ids WHERE card_id = p_id;
    DELETE FROM card_prices WHERE card_id = p_id;
    DELETE FROM card_related_uris WHERE card_id = p_id;
    DELETE FROM card_purchase_uris WHERE card_id = p_id;

    -- Insert multiverse IDs
    IF p_multiverse_ids IS NOT NULL THEN
        INSERT INTO card_multiverse_ids (card_id, multiverse_id)
        SELECT p_id, JSON_UNQUOTE(JSON_EXTRACT(p_multiverse_ids, CONCAT('$[', numbers.n, ']')))
        FROM (SELECT 0 n UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) numbers
        WHERE JSON_EXTRACT(p_multiverse_ids, CONCAT('$[', numbers.n, ']')) IS NOT NULL;
    END IF;

    -- Insert image URIs
    IF p_image_uris IS NOT NULL THEN
        INSERT INTO card_image_uris (card_id, small_uri, normal_uri, large_uri, png_uri, art_crop_uri, border_crop_uri)
        VALUES (
            p_id,
            JSON_UNQUOTE(JSON_EXTRACT(p_image_uris, '$.small')),
            JSON_UNQUOTE(JSON_EXTRACT(p_image_uris, '$.normal')),
            JSON_UNQUOTE(JSON_EXTRACT(p_image_uris, '$.large')),
            JSON_UNQUOTE(JSON_EXTRACT(p_image_uris, '$.png')),
            JSON_UNQUOTE(JSON_EXTRACT(p_image_uris, '$.art_crop')),
            JSON_UNQUOTE(JSON_EXTRACT(p_image_uris, '$.border_crop'))
        );
    END IF;

    -- Insert colors
    IF p_colors IS NOT NULL THEN
        INSERT INTO card_colors (card_id, color)
        SELECT p_id, JSON_UNQUOTE(JSON_EXTRACT(p_colors, CONCAT('$[', numbers.n, ']')))
        FROM (SELECT 0 n UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4) numbers
        WHERE JSON_EXTRACT(p_colors, CONCAT('$[', numbers.n, ']')) IS NOT NULL;
    END IF;

    -- Insert color identity
    IF p_color_identity IS NOT NULL THEN
        INSERT INTO card_color_identity (card_id, color)
        SELECT p_id, JSON_UNQUOTE(JSON_EXTRACT(p_color_identity, CONCAT('$[', numbers.n, ']')))
        FROM (SELECT 0 n UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4) numbers
        WHERE JSON_EXTRACT(p_color_identity, CONCAT('$[', numbers.n, ']')) IS NOT NULL;
    END IF;

    -- Insert keywords
    IF p_keywords IS NOT NULL THEN
        INSERT INTO card_keywords (card_id, keyword)
        SELECT p_id, JSON_UNQUOTE(JSON_EXTRACT(p_keywords, CONCAT('$[', numbers.n, ']')))
        FROM (SELECT 0 n UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) numbers
        WHERE JSON_EXTRACT(p_keywords, CONCAT('$[', numbers.n, ']')) IS NOT NULL;
    END IF;

    -- Insert produced mana
    IF p_produced_mana IS NOT NULL THEN
        INSERT INTO card_produced_mana (card_id, mana_symbol)
        SELECT p_id, JSON_UNQUOTE(JSON_EXTRACT(p_produced_mana, CONCAT('$[', numbers.n, ']')))
        FROM (SELECT 0 n UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4) numbers
        WHERE JSON_EXTRACT(p_produced_mana, CONCAT('$[', numbers.n, ']')) IS NOT NULL;
    END IF;

    -- Insert legalities
    IF p_legalities IS NOT NULL THEN
        INSERT INTO card_legalities (card_id, format_name, legality)
        SELECT p_id, 'standard', JSON_UNQUOTE(JSON_EXTRACT(p_legalities, '$.standard'))
        WHERE JSON_EXTRACT(p_legalities, '$.standard') IS NOT NULL
        UNION ALL
        SELECT p_id, 'future', JSON_UNQUOTE(JSON_EXTRACT(p_legalities, '$.future'))
        WHERE JSON_EXTRACT(p_legalities, '$.future') IS NOT NULL
        UNION ALL
        SELECT p_id, 'historic', JSON_UNQUOTE(JSON_EXTRACT(p_legalities, '$.historic'))
        WHERE JSON_EXTRACT(p_legalities, '$.historic') IS NOT NULL
        UNION ALL
        SELECT p_id, 'timeless', JSON_UNQUOTE(JSON_EXTRACT(p_legalities, '$.timeless'))
        WHERE JSON_EXTRACT(p_legalities, '$.timeless') IS NOT NULL
        UNION ALL
        SELECT p_id, 'gladiator', JSON_UNQUOTE(JSON_EXTRACT(p_legalities, '$.gladiator'))
        WHERE JSON_EXTRACT(p_legalities, '$.gladiator') IS NOT NULL
        UNION ALL
        SELECT p_id, 'pioneer', JSON_UNQUOTE(JSON_EXTRACT(p_legalities, '$.pioneer'))
        WHERE JSON_EXTRACT(p_legalities, '$.pioneer') IS NOT NULL
        UNION ALL
        SELECT p_id, 'modern', JSON_UNQUOTE(JSON_EXTRACT(p_legalities, '$.modern'))
        WHERE JSON_EXTRACT(p_legalities, '$.modern') IS NOT NULL
        UNION ALL
        SELECT p_id, 'legacy', JSON_UNQUOTE(JSON_EXTRACT(p_legalities, '$.legacy'))
        WHERE JSON_EXTRACT(p_legalities, '$.legacy') IS NOT NULL
        UNION ALL
        SELECT p_id, 'pauper', JSON_UNQUOTE(JSON_EXTRACT(p_legalities, '$.pauper'))
        WHERE JSON_EXTRACT(p_legalities, '$.pauper') IS NOT NULL
        UNION ALL
        SELECT p_id, 'vintage', JSON_UNQUOTE(JSON_EXTRACT(p_legalities, '$.vintage'))
        WHERE JSON_EXTRACT(p_legalities, '$.vintage') IS NOT NULL
        UNION ALL
        SELECT p_id, 'penny', JSON_UNQUOTE(JSON_EXTRACT(p_legalities, '$.penny'))
        WHERE JSON_EXTRACT(p_legalities, '$.penny') IS NOT NULL
        UNION ALL
        SELECT p_id, 'commander', JSON_UNQUOTE(JSON_EXTRACT(p_legalities, '$.commander'))
        WHERE JSON_EXTRACT(p_legalities, '$.commander') IS NOT NULL
        UNION ALL
        SELECT p_id, 'oathbreaker', JSON_UNQUOTE(JSON_EXTRACT(p_legalities, '$.oathbreaker'))
        WHERE JSON_EXTRACT(p_legalities, '$.oathbreaker') IS NOT NULL
        UNION ALL
        SELECT p_id, 'standardbrawl', JSON_UNQUOTE(JSON_EXTRACT(p_legalities, '$.standardbrawl'))
        WHERE JSON_EXTRACT(p_legalities, '$.standardbrawl') IS NOT NULL
        UNION ALL
        SELECT p_id, 'brawl', JSON_UNQUOTE(JSON_EXTRACT(p_legalities, '$.brawl'))
        WHERE JSON_EXTRACT(p_legalities, '$.brawl') IS NOT NULL
        UNION ALL
        SELECT p_id, 'alchemy', JSON_UNQUOTE(JSON_EXTRACT(p_legalities, '$.alchemy'))
        WHERE JSON_EXTRACT(p_legalities, '$.alchemy') IS NOT NULL
        UNION ALL
        SELECT p_id, 'paupercommander', JSON_UNQUOTE(JSON_EXTRACT(p_legalities, '$.paupercommander'))
        WHERE JSON_EXTRACT(p_legalities, '$.paupercommander') IS NOT NULL
        UNION ALL
        SELECT p_id, 'duel', JSON_UNQUOTE(JSON_EXTRACT(p_legalities, '$.duel'))
        WHERE JSON_EXTRACT(p_legalities, '$.duel') IS NOT NULL
        UNION ALL
        SELECT p_id, 'oldschool', JSON_UNQUOTE(JSON_EXTRACT(p_legalities, '$.oldschool'))
        WHERE JSON_EXTRACT(p_legalities, '$.oldschool') IS NOT NULL
        UNION ALL
        SELECT p_id, 'premodern', JSON_UNQUOTE(JSON_EXTRACT(p_legalities, '$.premodern'))
        WHERE JSON_EXTRACT(p_legalities, '$.premodern') IS NOT NULL
        UNION ALL
        SELECT p_id, 'predh', JSON_UNQUOTE(JSON_EXTRACT(p_legalities, '$.predh'))
        WHERE JSON_EXTRACT(p_legalities, '$.predh') IS NOT NULL;
    END IF;

    -- Insert games
    IF p_games IS NOT NULL THEN
        INSERT INTO card_games (card_id, game)
        SELECT p_id, JSON_UNQUOTE(JSON_EXTRACT(p_games, CONCAT('$[', numbers.n, ']')))
        FROM (SELECT 0 n UNION SELECT 1 UNION SELECT 2 UNION SELECT 3) numbers
        WHERE JSON_EXTRACT(p_games, CONCAT('$[', numbers.n, ']')) IS NOT NULL;
    END IF;

    -- Insert finishes
    IF p_finishes IS NOT NULL THEN
        INSERT INTO card_finishes (card_id, finish)
        SELECT p_id, JSON_UNQUOTE(JSON_EXTRACT(p_finishes, CONCAT('$[', numbers.n, ']')))
        FROM (SELECT 0 n UNION SELECT 1 UNION SELECT 2) numbers
        WHERE JSON_EXTRACT(p_finishes, CONCAT('$[', numbers.n, ']')) IS NOT NULL;
    END IF;

    -- Insert artist IDs
    IF p_artist_ids IS NOT NULL THEN
        INSERT INTO card_artist_ids (card_id, artist_id)
        SELECT p_id, JSON_UNQUOTE(JSON_EXTRACT(p_artist_ids, CONCAT('$[', numbers.n, ']')))
        FROM (SELECT 0 n UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4) numbers
        WHERE JSON_EXTRACT(p_artist_ids, CONCAT('$[', numbers.n, ']')) IS NOT NULL;
    END IF;

    -- Insert prices
    IF p_prices IS NOT NULL THEN
        INSERT INTO card_prices (card_id, usd, usd_foil, usd_etched, eur, eur_foil, tix)
        VALUES (
            p_id,
            CASE WHEN JSON_UNQUOTE(JSON_EXTRACT(p_prices, '$.usd')) = 'null' THEN NULL 
                 ELSE CAST(JSON_UNQUOTE(JSON_EXTRACT(p_prices, '$.usd')) AS DECIMAL(10,2)) END,
            CASE WHEN JSON_UNQUOTE(JSON_EXTRACT(p_prices, '$.usd_foil')) = 'null' THEN NULL 
                 ELSE CAST(JSON_UNQUOTE(JSON_EXTRACT(p_prices, '$.usd_foil')) AS DECIMAL(10,2)) END,
            CASE WHEN JSON_UNQUOTE(JSON_EXTRACT(p_prices, '$.usd_etched')) = 'null' THEN NULL 
                 ELSE CAST(JSON_UNQUOTE(JSON_EXTRACT(p_prices, '$.usd_etched')) AS DECIMAL(10,2)) END,
            CASE WHEN JSON_UNQUOTE(JSON_EXTRACT(p_prices, '$.eur')) = 'null' THEN NULL 
                 ELSE CAST(JSON_UNQUOTE(JSON_EXTRACT(p_prices, '$.eur')) AS DECIMAL(10,2)) END,
            CASE WHEN JSON_UNQUOTE(JSON_EXTRACT(p_prices, '$.eur_foil')) = 'null' THEN NULL 
                 ELSE CAST(JSON_UNQUOTE(JSON_EXTRACT(p_prices, '$.eur_foil')) AS DECIMAL(10,2)) END,
            CASE WHEN JSON_UNQUOTE(JSON_EXTRACT(p_prices, '$.tix')) = 'null' THEN NULL 
                 ELSE CAST(JSON_UNQUOTE(JSON_EXTRACT(p_prices, '$.tix')) AS DECIMAL(10,2)) END
        );
    END IF;

    -- Insert related URIs
    IF p_related_uris IS NOT NULL THEN
        INSERT INTO card_related_uris (card_id, gatherer, tcgplayer_infinite_articles, tcgplayer_infinite_decks, edhrec)
        VALUES (
            p_id,
            JSON_UNQUOTE(JSON_EXTRACT(p_related_uris, '$.gatherer')),
            JSON_UNQUOTE(JSON_EXTRACT(p_related_uris, '$.tcgplayer_infinite_articles')),
            JSON_UNQUOTE(JSON_EXTRACT(p_related_uris, '$.tcgplayer_infinite_decks')),
            JSON_UNQUOTE(JSON_EXTRACT(p_related_uris, '$.edhrec'))
        );
    END IF;

    -- Insert purchase URIs
    IF p_purchase_uris IS NOT NULL THEN
        INSERT INTO card_purchase_uris (card_id, tcgplayer, cardmarket, cardhoarder)
        VALUES (
            p_id,
            JSON_UNQUOTE(JSON_EXTRACT(p_purchase_uris, '$.tcgplayer')),
            JSON_UNQUOTE(JSON_EXTRACT(p_purchase_uris, '$.cardmarket')),
            JSON_UNQUOTE(JSON_EXTRACT(p_purchase_uris, '$.cardhoarder'))
        );
    END IF;

    COMMIT;
END //

DELIMITER ;