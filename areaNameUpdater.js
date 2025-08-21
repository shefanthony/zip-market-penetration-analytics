const fs = require('fs');
const csv = require('csv-parser');
const axios = require('axios');

class AreaNameUpdater {
    constructor() {
        this.nycData = new Map();
        this.njData = new Map();
        this.zipToAreaName = new Map();
    }

    async loadNYCData(filePath) {
        return new Promise((resolve, reject) => {
            fs.createReadStream(filePath)
                .pipe(csv())
                .on('data', (row) => {
                    const zip = row.zip;
                    const areaName = row.neighborhood || row.borough || row.post_office;
                    if (zip && areaName) {
                        this.nycData.set(zip, areaName);
                    }
                })
                .on('end', () => {
                    console.log(`Loaded ${this.nycData.size} NYC ZIP codes`);
                    resolve();
                })
                .on('error', reject);
        });
    }

    async loadNJData(filePath) {
        return new Promise((resolve, reject) => {
            fs.createReadStream(filePath)
                .pipe(csv())
                .on('data', (row) => {
                    // NJ file doesn't have ZIP codes directly, we'll need to map them
                    const municipalityName = row.MUNICIPALITY_NAME_COMMON;
                    const countyName = row.COUNTY_NAME_COMMON;
                    if (municipalityName && countyName) {
                        // Store for later ZIP code mapping
                        this.njData.set(municipalityName.toLowerCase(), {
                            municipality: municipalityName,
                            county: countyName
                        });
                    }
                })
                .on('end', () => {
                    console.log(`Loaded ${this.njData.size} NJ municipalities`);
                    resolve();
                })
                .on('error', reject);
        });
    }

    async searchZipCodeAreaName(zipCode) {
        try {
            // Try to find area name via web search
            console.log(`Searching for area name for ZIP ${zipCode}...`);
            
            // We'll use a simple approach - search for the ZIP code and extract location info
            const searchQuery = `ZIP code ${zipCode} neighborhood city town name location`;
            
            // For now, let's try to determine state from ZIP code patterns
            let state = '';
            const zipNum = parseInt(zipCode);
            
            if (zipNum >= 10000 && zipNum <= 14999) {
                state = 'NY';
            } else if (zipNum >= 7000 && zipNum <= 8999) {
                state = 'NJ';
            } else if (zipNum >= 6000 && zipNum <= 6999) {
                state = 'CT';
            }

            // For demonstration, let's create a basic lookup for common patterns
            const commonZipPatterns = {
                // Manhattan patterns
                '100': 'Manhattan',
                '101': 'Manhattan', 
                '102': 'Manhattan',
                '103': 'Manhattan',
                // Brooklyn patterns  
                '112': 'Brooklyn',
                '116': 'Brooklyn',
                // NJ patterns
                '070': 'New Jersey',
                '071': 'New Jersey',
                '072': 'New Jersey',
                '073': 'New Jersey',
                '074': 'New Jersey',
                '075': 'New Jersey',
                '076': 'New Jersey',
                '077': 'New Jersey',
                '078': 'New Jersey',
                '079': 'New Jersey',
                '080': 'New Jersey',
                '081': 'New Jersey',
                '082': 'New Jersey',
                '083': 'New Jersey',
                '084': 'New Jersey',
                '085': 'New Jersey',
                '086': 'New Jersey',
                '087': 'New Jersey',
                '088': 'New Jersey',
                '089': 'New Jersey'
            };

            const prefix = zipCode.substring(0, 3);
            if (commonZipPatterns[prefix]) {
                return `${commonZipPatterns[prefix]} Area`;
            }

            return null;
        } catch (error) {
            console.warn(`Failed to search area name for ZIP ${zipCode}:`, error.message);
            return null;
        }
    }

    async searchSpecificZipCode(zipCode) {
        try {
            // Let's try to get more specific area names for key ZIP codes
            const specificMappings = {
                // Manhattan
                '10001': 'Chelsea',
                '10002': 'Lower East Side', 
                '10003': 'East Village',
                '10004': 'Financial District',
                '10005': 'Financial District',
                '10006': 'Financial District',
                '10007': 'Financial District',
                '10009': 'East Village',
                '10010': 'Gramercy',
                '10011': 'Chelsea',
                '10012': 'SoHo',
                '10013': 'SoHo',
                '10014': 'West Village',
                '10016': 'Murray Hill',
                '10017': 'Midtown East',
                '10018': 'Garment District',
                '10019': 'Hell\'s Kitchen',
                '10020': 'Midtown',
                '10021': 'Upper East Side',
                '10022': 'Midtown East',
                '10023': 'Upper West Side',
                '10024': 'Upper West Side',
                '10025': 'Upper West Side',
                '10026': 'Morningside Heights',
                '10027': 'Morningside Heights',
                '10028': 'Upper East Side',
                '10029': 'East Harlem',
                '10030': 'Harlem',
                '10031': 'Harlem',
                '10032': 'Harlem',
                '10033': 'Harlem',
                '10034': 'Inwood',
                '10035': 'East Harlem',
                '10036': 'Times Square',
                '10037': 'Harlem',
                '10038': 'Financial District',
                '10039': 'Harlem',
                '10040': 'Inwood',
                '10044': 'Roosevelt Island',
                '10065': 'Upper East Side',
                '10075': 'Upper East Side',
                '10128': 'Upper East Side',
                '10155': 'Midtown (Commercial)',
                '10162': 'Midtown (Commercial)',
                '10166': 'Midtown (Commercial)',
                '10280': 'Battery Park City',
                '10282': 'Battery Park City',

                // Brooklyn
                '11201': 'Brooklyn Heights',
                '11203': 'East Flatbush',
                '11204': 'Bensonhurst',
                '11205': 'Fort Greene',
                '11206': 'Williamsburg',
                '11207': 'East New York',
                '11208': 'East New York',
                '11209': 'Bay Ridge',
                '11210': 'Flatlands',
                '11211': 'Williamsburg',
                '11212': 'Brownsville',
                '11213': 'Crown Heights',
                '11214': 'Bensonhurst',
                '11215': 'Park Slope',
                '11216': 'Bedford-Stuyvesant',
                '11217': 'Boerum Hill',
                '11218': 'Kensington',
                '11219': 'Borough Park',
                '11220': 'Sunset Park',
                '11221': 'Bushwick',
                '11222': 'Greenpoint',
                '11223': 'Gravesend',
                '11224': 'Coney Island',
                '11225': 'Crown Heights',
                '11226': 'Flatbush',
                '11228': 'Bensonhurst',
                '11229': 'Sheepshead Bay',
                '11230': 'Midwood',
                '11231': 'Red Hook',
                '11232': 'Sunset Park',
                '11233': 'Bedford-Stuyvesant',
                '11234': 'Marine Park',
                '11235': 'Sheepshead Bay',
                '11236': 'Canarsie',
                '11237': 'Bushwick',
                '11238': 'Prospect Heights',
                '11243': 'Fort Greene (Commercial)',
                '11249': 'Williamsburg',

                // Queens  
                '11101': 'Long Island City',
                '11102': 'Astoria',
                '11103': 'Astoria',
                '11104': 'Sunnyside',
                '11105': 'Astoria',
                '11106': 'Astoria',
                '11109': 'Long Island City',
                '11354': 'Flushing',
                '11355': 'Flushing',
                '11356': 'College Point',
                '11357': 'Whitestone',
                '11358': 'Flushing',
                '11361': 'Bayside',
                '11362': 'Little Neck',
                '11363': 'Little Neck',
                '11364': 'Oakland Gardens',
                '11365': 'Fresh Meadows',
                '11366': 'Fresh Meadows',
                '11367': 'Flushing',
                '11368': 'Corona',
                '11369': 'East Elmhurst',
                '11370': 'East Elmhurst',
                '11372': 'Jackson Heights',
                '11373': 'Elmhurst',
                '11374': 'Rego Park',
                '11375': 'Forest Hills',
                '11377': 'Woodside',
                '11378': 'Maspeth',
                '11379': 'Middle Village',
                '11385': 'Ridgewood',
                '11415': 'Kew Gardens',
                '11416': 'Ozone Park',
                '11417': 'Ozone Park',
                '11418': 'Richmond Hill',
                '11419': 'South Richmond Hill',
                '11420': 'South Ozone Park',
                '11421': 'Woodhaven',
                '11422': 'Rosedale',
                '11423': 'Hollis',
                '11426': 'Bellerose',
                '11427': 'Queens Village',
                '11428': 'Queens Village',
                '11429': 'Queens Village',
                '11432': 'Jamaica',
                '11433': 'Jamaica',
                '11434': 'Jamaica',
                '11435': 'Jamaica',
                '11436': 'South Jamaica',

                // Bronx
                '10451': 'Highbridge',
                '10452': 'Morris Heights',
                '10453': 'Morris Heights', 
                '10454': 'Mott Haven',
                '10455': 'Melrose',
                '10456': 'Morrisania',
                '10457': 'Mount Hope',
                '10458': 'Fordham',
                '10459': 'Longwood',
                '10460': 'West Farms',
                '10461': 'Westchester Square',
                '10462': 'Parkchester',
                '10463': 'Riverdale',
                '10464': 'Riverdale',
                '10465': 'Country Club',
                '10466': 'Wakefield',
                '10467': 'Fordham',
                '10468': 'Fordham',
                '10469': 'Baychester',
                '10470': 'Wakefield',
                '10471': 'Riverdale',
                '10472': 'Soundview',
                '10473': 'Soundview',
                '10474': 'Hunts Point',
                '10475': 'Co-op City',

                // Staten Island
                '10301': 'St. George',
                '10302': 'Port Richmond',
                '10303': 'Stapleton',
                '10304': 'Stapleton',
                '10305': 'Arrochar',
                '10306': 'Tottenville',
                '10307': 'Charleston',
                '10308': 'Great Kills',
                '10309': 'Prince\'s Bay',
                '10310': 'Port Richmond',
                '10312': 'Annadale',
                '10314': 'Sunnyside'
            };

            if (specificMappings[zipCode]) {
                return specificMappings[zipCode];
            }

            // If not in our specific mappings, try the pattern-based approach
            return await this.searchZipCodeAreaName(zipCode);

        } catch (error) {
            console.warn(`Error searching for ZIP ${zipCode}:`, error.message);
            return null;
        }
    }

    async updateProcessedData() {
        try {
            // Load existing processed data
            const processedData = JSON.parse(fs.readFileSync('processed_data.json', 'utf8'));
            
            console.log('Updating processed data with area names...');
            
            for (let i = 0; i < processedData.length; i++) {
                const record = processedData[i];
                const zipCode = record.zipCode;
                
                let areaName = null;
                
                // First, check NYC data
                if (this.nycData.has(zipCode)) {
                    areaName = this.nycData.get(zipCode);
                } else {
                    // Search for area name
                    areaName = await this.searchSpecificZipCode(zipCode);
                }
                
                record.areaName = areaName || 'Unknown Area';
                
                if (i % 50 === 0) {
                    console.log(`Processed ${i + 1}/${processedData.length} records...`);
                }
            }
            
            // Save updated data
            fs.writeFileSync('processed_data.json', JSON.stringify(processedData, null, 2));
            console.log('Successfully updated processed data with area names');
            
            return processedData;
            
        } catch (error) {
            console.error('Error updating processed data:', error);
            throw error;
        }
    }

    async run() {
        try {
            console.log('Starting area name update process...');
            
            // Load reference data
            await this.loadNYCData('/Users/anthonymangia/Downloads/nyc_zip_borough_neighborhoods_pop.csv');
            await this.loadNJData('/Users/anthonymangia/Downloads/Municipalities_of_New_Jersey_20250820.csv');
            
            // Update processed data with area names
            await this.updateProcessedData();
            
            console.log('Area name update process completed!');
            
        } catch (error) {
            console.error('Error in area name update process:', error);
            throw error;
        }
    }
}

module.exports = AreaNameUpdater;

// Run if called directly
if (require.main === module) {
    const updater = new AreaNameUpdater();
    updater.run().catch(console.error);
}