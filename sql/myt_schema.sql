

create table if not exists set
(
	id              varchar not null constraint set_pkey primary key,
	name            varchar not null,
	set_type        varchar,
	released_at     varchar,
	block_code      varchar,
	block           varchar,
	parent_set_code varchar,
	card_count      integer,
	digital         boolean,
	foil_only       boolean,
	nonfoil_only    boolean,
	scryfall_uri    varchar,
	uri             varchar,
	icon_svg_uri    varchar,
	search_uri      varchar
);

alter table set owner to myt_user;

create table if not exists artist (
	id              serial not null constraint artist_pkey primary key,
	name            varchar not null constraint artist_name_key unique
);

alter table artist owner to myt_user;

create table if not exists import_status (
	id              serial not null constraint import_status_pkey primary key,
	started_at      timestamp,
	completed_at    timestamp,
	status          varchar,
	total_cards     integer,
	processed_cards integer,
	error_message   text
);

alter table import_status owner to myt_user;

create table if not exists card (
	id                  varchar not null constraint card_pkey primary key,
	oracle_id           varchar,
	multiverse_ids      text,
	mtgo_id             integer,
	mtgo_foil_id        integer,
	tcgplayer_id        integer,
	cardmarket_id       integer,
	name                varchar not null,
	lang                varchar,
	released_at         varchar,
	uri                 varchar,
	scryfall_uri        varchar,
	layout              varchar,
	image_status        varchar,
	image_uris          text,
	mana_cost           varchar,
	cmc                 double precision,
	type_line           varchar,
	oracle_text         text,
	flavor_text         text,
	power               varchar,
	toughness           varchar,
	loyalty             varchar,
	set_id              varchar constraint card_set_id_fkey references set,
	set_name            varchar,
	set_type            varchar,
	set_uri             varchar,
	set_search_uri      varchar,
	scryfall_set_uri    varchar,
	rulings_uri         varchar,
	prints_search_uri   varchar,
	collector_number    varchar,
	digital             boolean,
	rarity              varchar,
	artist_id           integer constraint card_artist_id_fkey references artist,
	illustration_id     varchar,
	border_color        varchar,
	frame               varchar,
	frame_effects       text,
	security_stamp      varchar,
	full_art            boolean,
	textless            boolean,
	booster             boolean,
	story_spotlight     boolean,
	prices              text,
	purchase_uris       text,
	related_uris        text,
	data_hash           varchar(64)
);

alter table  card add data_hash varchar(64)

alter table card owner to myt_user;

create table if not exists card_colors (
	card_id             varchar not null constraint card_colors_card_id_fkey references card,
	color               varchar(1) not null,
	constraint          card_colors_pkey primary key (card_id, color)
);

alter table card_colors owner to myt_user;

create table if not exists card_color_identity (
	card_id             varchar not null constraint card_color_identity_card_id_fkey references card,
	color               varchar(1) not null,
	constraint          card_color_identity_pkey primary key (card_id, color)
);

alter table card_color_identity owner to myt_user;

create table if not exists card_types (
	card_id             varchar not null constraint card_types_card_id_fkey references card,
	type_name           varchar not null,
	constraint          card_types_pkey primary key (card_id, type_name)
);

alter table card_types owner to myt_user;

create table if not exists card_subtypes (
	card_id             varchar not null constraint card_subtypes_card_id_fkey references card,
	subtype_name        varchar not null,
	constraint          card_subtypes_pkey primary key (card_id, subtype_name)
);

alter table card_subtypes owner to myt_user;

create table if not exists card_supertypes (
	card_id             varchar not null constraint card_supertypes_card_id_fkey references card,
	supertype_name      varchar not null,
	constraint          card_supertypes_pkey primary key (card_id, supertype_name)
);

alter table card_supertypes owner to myt_user;

create table if not exists legality (
	id                  serial not null constraint legality_pkey primary key,
	card_id             varchar not null constraint legality_card_id_fkey references card,
	format_name         varchar not null,
	legality_status     varchar not null
);

alter table legality owner to myt_user;

