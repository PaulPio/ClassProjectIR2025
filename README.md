# Water Quality Monitoring Dashboard

A full-stack web application for monitoring ocean water quality data with interactive visualizations and real-time filtering.

## 🏗️ Architecture

- **Backend**: Flask API (Python) - serves water quality data from MongoDB
- **Frontend**: Streamlit Dashboard (Python) - interactive web interface
- **Database**: MongoDB - stores water quality measurements
- **Data Processing**: Python scripts for data cleaning and outlier detection

## 🚀 Quick Start

### Prerequisites
- Python 3.13+
- MongoDB Atlas account (or local MongoDB)
- Git

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd ClassProjectIR2025
   ```

2. **Create virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Set up environment variables**
   Create a `.env` file in the root directory:
   ```
   MONGO_USER=your_mongodb_username
   MONGO_PASS=your_mongodb_password
   MONGO_CLUSTER_URL=your_cluster_url.mongodb.net
   ```

### Running the Application

1. **Start the Flask API** (Terminal 1)
   ```bash
   source venv/bin/activate
   python api/app.py
   ```
   API will be available at: `http://localhost:5001`

2. **Start the Streamlit Dashboard** (Terminal 2)
   ```bash
   source venv/bin/activate
   streamlit run "Streamlit Client.py"
   ```
   Dashboard will be available at: `http://localhost:8501`

## 📊 Features

### Flask API Endpoints
- `GET /` - API status
- `GET /api/health` - Health check
- `GET /api/observations` - Water quality data with filtering
- `GET /api/stats` - Statistical summary
- `GET /api/outliers` - Outlier detection

### Streamlit Dashboard
- **Interactive Data Table** - Browse water quality measurements
- **Filter Controls** - Filter by temperature, salinity, and ODO ranges
- **Charts & Visualizations**:
  - Temperature over time (line chart)
  - Salinity distribution (histogram)
  - Temperature vs ODO scatter plot
  - Geographic map of sampling locations
- **Statistics Panel** - Summary statistics for all measurements
- **Pagination** - Navigate through large datasets

## 🔧 Data Processing

### Data Cleaning (`data/data_cleaning.py`)
- Z-score outlier detection (|z| > 3.0)
- Processes multiple CSV files
- Generates cleaning reports
- Retains ~97% of original data

### Database Loading (`data/load_to_db.py`)
- Loads cleaned data into MongoDB
- Handles environment variable configuration
- Provides data verification

## 📁 Project Structure

```
ClassProjectIR2025/
├── api/
│   └── app.py                 # Flask API server
├── data/
│   ├── data_cleaning.py      # Data cleaning scripts
│   ├── load_to_db.py         # Database loading
│   └── *.csv                 # Water quality data files
├── Streamlit Client.py       # Streamlit dashboard
├── config.json               # Dashboard configuration
├── requirements.txt          # Python dependencies
├── .env                      # Environment variables
└── README.md                 # This file
```

## 🌊 Data Schema

Water quality measurements include:
- **Temperature (c)** - Water temperature in Celsius
- **Salinity (ppt)** - Salinity in parts per thousand
- **ODO mg/L** - Dissolved oxygen in mg/L
- **Latitude/Longitude** - GPS coordinates
- **Time hh:mm:ss** - Timestamp
- **pH** - Water pH level
- **Additional sensors** - Battery, depth, GPS, etc.

## 🔍 API Usage Examples

### Get all observations
```bash
curl http://localhost:5001/api/observations
```

### Filter by temperature range
```bash
curl "http://localhost:5001/api/observations?min_temp=20&max_temp=30"
```

### Get statistics
```bash
curl http://localhost:5001/api/stats
```

## 🛠️ Development

### Branch Structure
- `integrated-fullstack-app` - Complete integrated application (this branch)
- `api-part2` - Flask API backend
- `SharleneElias13-patch-1` - Streamlit frontend
- `data-cleaning-part1` - Data processing scripts

### Key Integration Fixes
1. **Port Conflict Resolution** - Changed Flask from port 5000 to 5001
2. **Parameter Mapping** - Aligned frontend/backend parameter names
3. **Data Structure Alignment** - Fixed JSON response format
4. **Column Name Consistency** - Updated chart column references
5. **Dependency Management** - Added missing packages and requirements.txt

## 📈 Performance

- **Data Volume**: 1,735+ water quality measurements
- **Response Time**: <100ms for filtered queries
- **Concurrent Users**: Supports multiple dashboard sessions
- **Data Retention**: 97% after outlier removal


**Ready to explore ocean water quality data!** 🌊📊