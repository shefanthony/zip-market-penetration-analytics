const fs = require('fs');
const csv = require('csv-parser');
const axios = require('axios');

class DataProcessor {
    constructor(apiKey) {
        this.apiKey = apiKey;
        this.data = [];
        this.processedData = [];
    }

    async loadCSVData(filePath) {
        return new Promise((resolve, reject) => {
            const results = [];
            fs.createReadStream(filePath)
                .pipe(csv())
                .on('data', (data) => results.push(data))
                .on('end', () => {
                    this.data = results;
                    console.log(`Loaded ${results.length} records from CSV`);
                    resolve(results);
                })
                .on('error', reject);
        });
    }

    async fetchPopulationData(zipCode) {
        try {
            // Use 2023 ACS 5-year data (most recent available)
            const url = `https://api.census.gov/data/2023/acs/acs5?get=NAME,B01003_001E&for=zip%20code%20tabulation%20area:${zipCode}&key=${this.apiKey}`;
            
            const response = await axios.get(url, {
                timeout: 10000,
                headers: {
                    'User-Agent': 'ZIP-Penetration-Analysis/1.0'
                }
            });

            if (response.data && response.data.length > 1) {
                const population = parseInt(response.data[1][1]);
                return isNaN(population) ? null : population;
            }
            return null;
        } catch (error) {
            console.warn(`Failed to fetch population for ZIP ${zipCode}:`, error.message);
            return null;
        }
    }

    async processAllData() {
        console.log('Processing data and fetching population information...');
        const processed = [];
        
        // Process in batches to avoid rate limiting
        const batchSize = 10;
        const delay = 100; // 100ms delay between requests
        
        for (let i = 0; i < this.data.length; i += batchSize) {
            const batch = this.data.slice(i, i + batchSize);
            const batchPromises = batch.map(async (row) => {
                const zipCode = row.ZIP_CODE;
                const population = await this.fetchPopulationData(zipCode);
                
                // Calculate market penetration
                const netMV = parseFloat(row.NET_MV) || 0;
                const orderCount = parseInt(row.ORDER_COUNT) || 0;
                
                // Market penetration = (Order Count / Population) * 100
                const marketPenetration = population && population > 0 ? 
                    ((orderCount / population) * 100).toFixed(6) : null;
                
                return {
                    zipCode,
                    deliveryType: row.DELIVERY_TYPE,
                    netMV,
                    orderCount,
                    population,
                    marketPenetration: marketPenetration ? parseFloat(marketPenetration) : null,
                    tuesdayNetMV: parseFloat(row.TUE_NET_MV) || 0,
                    tuesdayPct: parseFloat(row.TUE_MV_PCT) || 0,
                    wednesdayNetMV: parseFloat(row.WED_NET_MV) || 0,
                    wednesdayPct: parseFloat(row.WED_MV_PCT) || 0,
                    thursdayNetMV: parseFloat(row.THU_NET_MV) || 0,
                    thursdayPct: parseFloat(row.THU_MV_PCT) || 0,
                    sundayNetMV: parseFloat(row.SUN_NET_MV) || 0,
                    sundayPct: parseFloat(row.SUN_MV_PCT) || 0
                };
            });
            
            const batchResults = await Promise.all(batchPromises);
            processed.push(...batchResults);
            
            console.log(`Processed batch ${Math.floor(i/batchSize) + 1}/${Math.ceil(this.data.length/batchSize)}`);
            
            // Add delay between batches
            if (i + batchSize < this.data.length) {
                await new Promise(resolve => setTimeout(resolve, delay));
            }
        }
        
        this.processedData = processed;
        console.log(`Processing complete. ${processed.length} records processed.`);
        
        // Save processed data to file
        await this.saveProcessedData();
        
        return processed;
    }

    async saveProcessedData() {
        const filePath = 'processed_data.json';
        try {
            fs.writeFileSync(filePath, JSON.stringify(this.processedData, null, 2));
            console.log(`Processed data saved to ${filePath}`);
        } catch (error) {
            console.error('Error saving processed data:', error);
        }
    }

    getStatistics() {
        if (this.processedData.length === 0) return null;
        
        const validPenetrationData = this.processedData.filter(d => d.marketPenetration !== null);
        
        if (validPenetrationData.length === 0) return null;
        
        const penetrationValues = validPenetrationData.map(d => d.marketPenetration);
        const totalOrders = this.processedData.reduce((sum, d) => sum + d.orderCount, 0);
        const totalPopulation = this.processedData.reduce((sum, d) => sum + (d.population || 0), 0);
        
        return {
            totalZipCodes: this.processedData.length,
            zipCodesWithPopulationData: validPenetrationData.length,
            totalOrders,
            totalPopulation,
            averageMarketPenetration: (penetrationValues.reduce((sum, val) => sum + val, 0) / penetrationValues.length).toFixed(6),
            maxMarketPenetration: Math.max(...penetrationValues).toFixed(6),
            minMarketPenetration: Math.min(...penetrationValues).toFixed(6),
            overallMarketPenetration: totalPopulation > 0 ? ((totalOrders / totalPopulation) * 100).toFixed(6) : null
        };
    }
}

module.exports = DataProcessor;