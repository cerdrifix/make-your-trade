# Overview

This is a Magic: The Gathering (MTG) card database application built with Flask that imports and manages MTG card data from the Scryfall API. The application provides a web interface for browsing cards, sets, and artists, with background data import capabilities from Scryfall's comprehensive card database.

# User Preferences

Preferred communication style: Simple, everyday language.

# System Architecture

## Backend Architecture
- **Flask Framework**: Core web application framework with SQLAlchemy ORM for database operations
- **Database Layer**: PostgreSQL database with SQLAlchemy models defining relationships between cards, sets, artists, and legalities
- **Background Processing**: Threading-based background import system for handling large dataset imports from Scryfall API
- **Data Import Strategy**: Batch processing with progress tracking and status monitoring through ImportStatus model

## Database Design
- **Relational Schema**: Well-structured relationships between cards, sets, artists, and legalities
- **Many-to-Many Relationships**: Association tables for card colors, types, subtypes, and supertypes
- **Import Tracking**: Dedicated ImportStatus table for monitoring background import operations
- **Data Integrity**: Foreign key constraints and proper indexing for performance

## Frontend Architecture
- **Template Engine**: Jinja2 templating with Bootstrap for responsive UI
- **Real-time Updates**: JavaScript polling mechanism for import status updates
- **Component Structure**: Modular template inheritance with base layout and specialized views
- **User Interface**: Dashboard-style interface with statistics cards, search functionality, and data browsing

## Data Management
- **External Data Source**: Scryfall API as primary data provider for MTG card information
- **Batch Import System**: Configurable batch size processing (1000 records) with error handling
- **Status Tracking**: Real-time progress monitoring with percentage completion and error logging
- **Data Persistence**: Full card data storage including metadata, images, and relationships

# External Dependencies

## Core Framework Dependencies
- **Flask**: Web application framework
- **SQLAlchemy/Flask-SQLAlchemy**: ORM and database abstraction layer
- **Werkzeug**: WSGI utilities including ProxyFix middleware

## Frontend Dependencies
- **Bootstrap**: CSS framework for responsive design (via CDN)
- **Feather Icons**: Icon library for UI elements (via CDN)
- **Custom JavaScript**: Real-time status polling and UI updates

## Data Sources
- **Scryfall API**: Primary data source for MTG card information
- **PostgreSQL**: Production database system
- **JSON Data Format**: Structured card data from Scryfall's bulk data exports

## Infrastructure
- **Threading**: Python threading for background data import operations
- **HTTP Requests**: API communication with external services
- **Environment Variables**: Configuration management for database URLs and session secrets