// Auto-refresh functionality for import status page
function startStatusPolling(importId) {
    const pollInterval = 2000; // Poll every 2 seconds
    
    function updateStatus() {
        fetch(`/api/import_status/${importId}`)
            .then(response => response.json())
            .then(data => {
                // Update status badge
                const statusBadge = document.getElementById('status-badge');
                if (statusBadge) {
                    statusBadge.textContent = data.status.charAt(0).toUpperCase() + data.status.slice(1);
                    statusBadge.className = `badge bg-${getBadgeClass(data.status)}`;
                }
                
                // Update progress bar if cards are being processed
                if (data.total_cards > 0) {
                    const progressBar = document.getElementById('progress-bar');
                    const progressText = document.getElementById('progress-text');
                    
                    if (progressBar && progressText) {
                        progressBar.style.width = data.progress_percentage + '%';
                        progressText.textContent = `${data.processed_cards.toLocaleString()} / ${data.total_cards.toLocaleString()} (${data.progress_percentage}%)`;
                    }
                    
                    // Update sidebar stats
                    const totalCards = document.getElementById('total-cards');
                    const processedCards = document.getElementById('processed-cards');
                    const remainingCards = document.getElementById('remaining-cards');
                    
                    if (totalCards) {
                        totalCards.textContent = data.total_cards.toLocaleString();
                    }
                    if (processedCards) {
                        processedCards.textContent = data.processed_cards.toLocaleString();
                    }
                    if (remainingCards) {
                        remainingCards.textContent = (data.total_cards - data.processed_cards).toLocaleString();
                    }
                }
                
                // Update completion time
                if (data.completed_at) {
                    const completedTime = document.getElementById('completed-time');
                    if (completedTime) {
                        const date = new Date(data.completed_at);
                        completedTime.textContent = date.toLocaleString();
                    }
                }
                
                // Stop polling if import is complete or failed
                if (data.status === 'completed' || data.status === 'failed') {
                    clearInterval(pollInterval);
                    
                    // Refresh page after a short delay to show final state
                    setTimeout(() => {
                        window.location.reload();
                    }, 2000);
                }
            })
            .catch(error => {
                console.error('Error polling status:', error);
            });
    }
    
    // Start polling
    const intervalId = setInterval(updateStatus, pollInterval);
    
    // Initial update
    updateStatus();
}

function getBadgeClass(status) {
    switch (status) {
        case 'completed':
            return 'success';
        case 'running':
            return 'warning';
        case 'failed':
            return 'danger';
        default:
            return 'secondary';
    }
}

// Initialize feather icons after dynamic content updates
function initializeFeatherIcons() {
    if (typeof feather !== 'undefined') {
        feather.replace();
    }
}

// Call on page load
document.addEventListener('DOMContentLoaded', function() {
    initializeFeatherIcons();
});

// Search form enhancements
document.addEventListener('DOMContentLoaded', function() {
    const searchForm = document.querySelector('form[method="GET"]');
    if (searchForm) {
        const searchInput = searchForm.querySelector('input[name="search"]');
        if (searchInput) {
            searchInput.addEventListener('keypress', function(e) {
                if (e.key === 'Enter') {
                    searchForm.submit();
                }
            });
        }
    }
});
