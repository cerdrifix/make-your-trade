from flask import render_template, request, jsonify, flash, redirect, url_for
from app import app, db
from models import Card, Set, Artist, Legality, ImportStatus
from data_importer import DataImporter
import threading
import logging

logger = logging.getLogger(__name__)

@app.route('/')
def index():
    """Main dashboard showing database stats and import controls."""
    # Get database statistics
    card_count = Card.query.count()
    set_count = Set.query.count()
    artist_count = Artist.query.count()
    
    # Get latest import status
    latest_import = ImportStatus.query.order_by(ImportStatus.id.desc()).first()
    
    return render_template('index.html', 
                         card_count=card_count,
                         set_count=set_count,
                         artist_count=artist_count,
                         latest_import=latest_import)

@app.route('/start_import', methods=['POST'])
def start_import():
    """Start the data import process in a background thread."""
    try:
        # Check if there's already an import running
        running_import = ImportStatus.query.filter_by(status='running').first()
        if running_import:
            flash('An import is already running!', 'warning')
            return redirect(url_for('index'))
        
        # Create new import status record
        import_status = ImportStatus(status='pending')
        db.session.add(import_status)
        db.session.commit()
        
        # Start import in background thread
        importer = DataImporter()
        thread = threading.Thread(target=importer.import_data, args=(import_status.id,))
        thread.daemon = True
        thread.start()
        
        flash('Data import started successfully!', 'success')
        return redirect(url_for('import_status', import_id=import_status.id))
        
    except Exception as e:
        logger.error(f"Error starting import: {str(e)}")
        flash(f'Error starting import: {str(e)}', 'danger')
        return redirect(url_for('index'))

@app.route('/import_status/<int:import_id>')
def import_status(import_id):
    """Show import progress page."""
    import_record = ImportStatus.query.get_or_404(import_id)
    return render_template('import_status.html', import_record=import_record)

@app.route('/api/import_status/<int:import_id>')
def api_import_status(import_id):
    """API endpoint to get import status for AJAX updates."""
    import_record = ImportStatus.query.get_or_404(import_id)
    
    return jsonify({
        'id': import_record.id,
        'status': import_record.status,
        'total_cards': import_record.total_cards,
        'processed_cards': import_record.processed_cards,
        'progress_percentage': import_record.progress_percentage(),
        'error_message': import_record.error_message,
        'started_at': import_record.started_at.isoformat() if import_record.started_at else None,
        'completed_at': import_record.completed_at.isoformat() if import_record.completed_at else None
    })

@app.route('/view_data')
def view_data():
    """View imported card data with pagination and search."""
    page = request.args.get('page', 1, type=int)
    search = request.args.get('search', '', type=str)
    set_filter = request.args.get('set', '', type=str)
    
    # Build query
    query = Card.query
    
    if search:
        query = query.filter(Card.name.ilike(f'%{search}%'))
    
    if set_filter:
        query = query.filter(Card.set_id == set_filter)
    
    # Get paginated results
    cards = query.order_by(Card.name).paginate(
        page=page, per_page=20, error_out=False
    )
    
    # Get all sets for filter dropdown
    sets = Set.query.order_by(Set.name).all()
    
    return render_template('view_data.html', 
                         cards=cards, 
                         sets=sets,
                         search=search,
                         set_filter=set_filter)

@app.route('/card/<card_id>')
def card_detail(card_id):
    """Show detailed information for a specific card."""
    card = Card.query.get_or_404(card_id)
    return render_template('card_detail.html', card=card)

@app.route('/sets')
def sets():
    """Show all sets."""
    sets = Set.query.order_by(Set.name).all()
    return render_template('sets.html', sets=sets)

@app.route('/artists')
def artists():
    """Show all artists."""
    artists = Artist.query.order_by(Artist.name).all()
    return render_template('artists.html', artists=artists)

@app.route('/updateHashes', methods=['POST'])
def update_hashes():
    """Update hashes for all existing records in the database."""
    try:
        # Check if there's already an import or hash update running
        running_import = ImportStatus.query.filter_by(status='running').first()
        if running_import:
            flash('An import or hash update is already running!', 'warning')
            return redirect(url_for('index'))
        
        # Create new import status record for hash update
        import_status = ImportStatus(status='pending')
        db.session.add(import_status)
        db.session.commit()
        
        # Start hash update in background thread
        importer = DataImporter()
        thread = threading.Thread(target=importer.update_hashes, args=(import_status.id,))
        thread.daemon = True
        thread.start()
        
        flash('Hash update started successfully!', 'success')
        return redirect(url_for('import_status', import_id=import_status.id))
        
    except Exception as e:
        logger.error(f"Error starting hash update: {str(e)}")
        flash(f'Error starting hash update: {str(e)}', 'danger')
        return redirect(url_for('index'))
