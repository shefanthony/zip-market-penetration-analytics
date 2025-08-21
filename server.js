const express = require('express');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const DataProcessor = require('./dataProcessor');

const app = express();
const PORT = process.env.PORT || 3000;

// Your Census API key
const CENSUS_API_KEY = 'd0ab335e50692233570248b6ff3e01754cba4e4f';

// Secure password hash (you can change this)
const PASSWORD_HASH = 'a8b4c2d1e9f3g7h5i6j4k8l2m9n1o5p3q7r2s6t8u4v7w1x9y3z5'; // shefbabywoo2025

// Middleware
app.use(cors());
app.use(express.json());
app.use(express.static('public'));

// Simple hash function (for demo - in production use bcrypt)
function simpleHash(password) {
    let hash = 0;
    for (let i = 0; i < password.length; i++) {
        const char = password.charCodeAt(i);
        hash = ((hash << 5) - hash) + char;
        hash = hash & hash; // Convert to 32-bit integer
    }
    // Convert to hex and pad with predefined string for security
    return Math.abs(hash).toString(16) + 'a8b4c2d1e9f3g7h5i6j4k8l2m9n1o5p3q7r2s6t8u4v7w1x9y3z5';
}

// Login endpoint
app.post('/api/login', (req, res) => {
    try {
        const { password } = req.body;
        
        if (!password) {
            return res.status(400).json({ success: false, message: 'Password required' });
        }
        
        // Check password (in production, use proper password hashing like bcrypt)
        const inputHash = simpleHash(password);
        const correctHash = simpleHash('shefbabywoo2025');
        
        if (inputHash === correctHash) {
            // Generate a simple session token
            const sessionToken = Math.random().toString(36).substring(2, 15) + 
                                Math.random().toString(36).substring(2, 15);
            
            res.json({ 
                success: true, 
                token: sessionToken,
                message: 'Authentication successful' 
            });
        } else {
            res.status(401).json({ 
                success: false, 
                message: 'Invalid password' 
            });
        }
    } catch (error) {
        console.error('Login error:', error);
        res.status(500).json({ 
            success: false, 
            message: 'Internal server error' 
        });
    }
});

// Global data storage
let processedData = [];
let dataProcessor = null;

// Initialize data processor and load data
async function initializeData() {
    try {
        dataProcessor = new DataProcessor(CENSUS_API_KEY);
        
        // Check if we have already processed data
        if (fs.existsSync('processed_data.json')) {
            console.log('Loading existing processed data...');
            const jsonData = fs.readFileSync('processed_data.json', 'utf8');
            processedData = JSON.parse(jsonData);
            console.log(`Loaded ${processedData.length} processed records`);
        } else {
            console.log('Processing data for the first time...');
            await dataProcessor.loadCSVData('data.csv');
            processedData = await dataProcessor.processAllData();
        }
    } catch (error) {
        console.error('Error initializing data:', error);
        process.exit(1);
    }
}

// API Routes

// Get all data with optional filtering and sorting
app.get('/api/data', (req, res) => {
    try {
        let data = [...processedData];
        
        // Filtering
        const { 
            zipCode,
            areaName, 
            minPenetration, 
            maxPenetration, 
            minOrders, 
            maxOrders,
            minPopulation,
            maxPopulation,
            hideCommercial = 'true'
        } = req.query;
        
        // Hide commercial ZIP codes (population = 0) by default
        if (hideCommercial === 'true') {
            data = data.filter(d => d.population !== 0);
        }
        
        if (zipCode) {
            data = data.filter(d => d.zipCode.includes(zipCode));
        }
        
        if (areaName) {
            data = data.filter(d => d.areaName && d.areaName.toLowerCase().includes(areaName.toLowerCase()));
        }
        
        if (minPenetration) {
            data = data.filter(d => d.marketPenetration !== null && d.marketPenetration >= parseFloat(minPenetration));
        }
        
        if (maxPenetration) {
            data = data.filter(d => d.marketPenetration !== null && d.marketPenetration <= parseFloat(maxPenetration));
        }
        
        if (minOrders) {
            data = data.filter(d => d.orderCount >= parseInt(minOrders));
        }
        
        if (maxOrders) {
            data = data.filter(d => d.orderCount <= parseInt(maxOrders));
        }
        
        if (minPopulation) {
            data = data.filter(d => d.population !== null && d.population >= parseInt(minPopulation));
        }
        
        if (maxPopulation) {
            data = data.filter(d => d.population !== null && d.population <= parseInt(maxPopulation));
        }
        
        // Add calculated MV per order
        data = data.map(row => ({
            ...row,
            mvPerOrder: row.orderCount > 0 ? row.netMV / row.orderCount : 0
        }));

        // Sorting
        const { sortBy, sortOrder = 'asc' } = req.query;
        if (sortBy) {
            data.sort((a, b) => {
                let aVal = a[sortBy];
                let bVal = b[sortBy];
                
                // Handle null values
                if (aVal === null && bVal === null) return 0;
                if (aVal === null) return sortOrder === 'asc' ? 1 : -1;
                if (bVal === null) return sortOrder === 'asc' ? -1 : 1;
                
                // Sort
                if (typeof aVal === 'string') {
                    aVal = aVal.toLowerCase();
                    bVal = bVal.toLowerCase();
                }
                
                if (sortOrder === 'desc') {
                    return bVal > aVal ? 1 : bVal < aVal ? -1 : 0;
                } else {
                    return aVal > bVal ? 1 : aVal < bVal ? -1 : 0;
                }
            });
        }
        
        res.json({
            data,
            total: data.length,
            filters: req.query
        });
    } catch (error) {
        console.error('Error in /api/data:', error);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// Get statistics
app.get('/api/stats', (req, res) => {
    try {
        if (!dataProcessor) {
            // Calculate stats manually from processed data
            const validPenetrationData = processedData.filter(d => d.marketPenetration !== null);
            const penetrationValues = validPenetrationData.map(d => d.marketPenetration);
            const totalOrders = processedData.reduce((sum, d) => sum + d.orderCount, 0);
            const totalPopulation = processedData.reduce((sum, d) => sum + (d.population || 0), 0);
            
            const stats = {
                totalZipCodes: processedData.length,
                zipCodesWithPopulationData: validPenetrationData.length,
                totalOrders,
                totalPopulation,
                averageMarketPenetration: penetrationValues.length > 0 ? 
                    (penetrationValues.reduce((sum, val) => sum + val, 0) / penetrationValues.length).toFixed(6) : null,
                maxMarketPenetration: penetrationValues.length > 0 ? 
                    Math.max(...penetrationValues).toFixed(6) : null,
                minMarketPenetration: penetrationValues.length > 0 ? 
                    Math.min(...penetrationValues).toFixed(6) : null,
                overallMarketPenetration: totalPopulation > 0 ? 
                    ((totalOrders / totalPopulation) * 100).toFixed(6) : null
            };
            
            res.json(stats);
        } else {
            const stats = dataProcessor.getStatistics();
            res.json(stats);
        }
    } catch (error) {
        console.error('Error in /api/stats:', error);
        res.status(500).json({ error: 'Internal server error' });
    }
});


// Serve the main page
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Start server
async function startServer() {
    await initializeData();
    
    app.listen(PORT, () => {
        console.log(`Server running on http://localhost:${PORT}`);
        console.log(`Dataset loaded with ${processedData.length} ZIP codes`);
        
        if (dataProcessor) {
            const stats = dataProcessor.getStatistics();
            if (stats) {
                console.log(`Overall market penetration: ${stats.overallMarketPenetration}%`);
                console.log(`Average market penetration: ${stats.averageMarketPenetration}%`);
            }
        }
    });
}

startServer().catch(console.error);