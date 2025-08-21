const fs = require('fs');
const csv = require('csv-parser');

class NJAreaNameUpdater {
    constructor() {
        this.njZipData = new Map();
    }

    async loadNJZipData(filePath) {
        return new Promise((resolve, reject) => {
            fs.createReadStream(filePath)
                .pipe(csv())
                .on('data', (row) => {
                    const zipCode = row['ZIP Code'];
                    const city = row['City'];
                    const county = row['County'];
                    
                    if (zipCode && city) {
                        // Create area name as "City, County" for NJ
                        const areaName = county ? `${city}, ${county} County` : city;
                        this.njZipData.set(zipCode, areaName);
                    }
                })
                .on('end', () => {
                    console.log(`Loaded ${this.njZipData.size} NJ ZIP codes with city names`);
                    resolve();
                })
                .on('error', reject);
        });
    }

    async updateProcessedData() {
        try {
            // Load existing processed data
            const processedData = JSON.parse(fs.readFileSync('processed_data.json', 'utf8'));
            
            console.log('Updating NJ area names in processed data...');
            let updatedCount = 0;
            
            for (let record of processedData) {
                const zipCode = record.zipCode;
                
                // Check if this ZIP code is in our NJ data
                if (this.njZipData.has(zipCode)) {
                    const newAreaName = this.njZipData.get(zipCode);
                    const oldAreaName = record.areaName;
                    
                    record.areaName = newAreaName;
                    updatedCount++;
                    
                    console.log(`Updated ${zipCode}: "${oldAreaName}" → "${newAreaName}"`);
                }
            }
            
            // Save updated data
            fs.writeFileSync('processed_data.json', JSON.stringify(processedData, null, 2));
            console.log(`Successfully updated ${updatedCount} NJ area names`);
            
            return processedData;
            
        } catch (error) {
            console.error('Error updating NJ area names:', error);
            throw error;
        }
    }

    async run() {
        try {
            console.log('Starting NJ area name update process...');
            
            // Load NJ ZIP code reference data
            await this.loadNJZipData('/Users/anthonymangia/Downloads/Zip Code Reference - Sheet1.csv');
            
            // Update processed data with specific NJ area names
            await this.updateProcessedData();
            
            console.log('NJ area name update process completed!');
            
        } catch (error) {
            console.error('Error in NJ area name update process:', error);
            throw error;
        }
    }
}

module.exports = NJAreaNameUpdater;

// Run if called directly
if (require.main === module) {
    const updater = new NJAreaNameUpdater();
    updater.run().catch(console.error);
}