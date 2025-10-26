from flask import Flask, request, jsonify
from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

MONGO_USER = os.getenv("MONGO_USER")
MONGO_PASS = os.getenv("MONGO_PASS")
MONGO_CLUSTER_URL = os.getenv("MONGO_CLUSTER_URL")

uri= f"mongodb+srv://{MONGO_USER}:{MONGO_PASS}@{MONGO_CLUSTER_URL}/?retryWrites=true&w=majority&appName=CIS3590"

client = MongoClient(uri)
db= client['water_quality_data']

collection = db['asv_1']

@app.route('/')
def home():
    return "API is working"

@app.route('/api/health', methods = ['GET'])
def health():
    return jsonify({"status": "ok"}), 200


@app.route('/api/observations', methods=['GET'])
def get_observations():

    try:
        query = {}

        # Get parameters
        limit = int(request.args.get('limit', 100))
        skip = int(request.args.get('skip', 0))

        # Max limit
        if limit > 1000:
            limit = 1000

        # Temperature filters
        min_temp = request.args.get('min_temp')
        max_temp = request.args.get('max_temp')
        if min_temp or max_temp:
            query['Temperature (c)'] = {}
            if min_temp:
                query['Temperature (c)']['$gte'] = float(min_temp)
            if max_temp:
                query['Temperature (c)']['$lte'] = float(max_temp)

        # Salinity filters
        min_sal = request.args.get('min_sal')
        max_sal = request.args.get('max_sal')
        if min_sal or max_sal:
            query['Salinity (ppt)'] = {}
            if min_sal:
                query['Salinity (ppt)']['$gte'] = float(min_sal)
            if max_sal:
                query['Salinity (ppt)']['$lte'] = float(max_sal)

        # ODO filters
        min_odo = request.args.get('min_odo')
        max_odo = request.args.get('max_odo')
        if min_odo or max_odo:
            query['ODO mg/L'] = {}
            if min_odo:
                query['ODO mg/L']['$gte'] = float(min_odo)
            if max_odo:
                query['ODO mg/L']['$lte'] = float(max_odo)

        # Execute query
        cursor = collection.find(query).limit(limit).skip(skip)
        items = []
        for doc in cursor:
            doc['_id'] = str(doc['_id'])
            items.append(doc)

        count = collection.count_documents(query)

        return jsonify({"count": count, "items": items}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/stats', methods=['GET'])
def get_stats():

    try:
        pipeline = [
            {
                '$group': {
                    '_id': None,
                    'temp_mean': {'$avg': '$Temperature (c)'},
                    'temp_min': {'$min': '$Temperature (c)'},
                    'temp_max': {'$max': '$Temperature (c)'},
                    'sal_mean': {'$avg': '$Salinity (ppt)'},
                    'sal_min': {'$min': '$Salinity (ppt)'},
                    'sal_max': {'$max': '$Salinity (ppt)'},
                    'odo_mean': {'$avg': '$ODO mg/L'},
                    'odo_min': {'$min': '$ODO mg/L'},
                    'odo_max': {'$max': '$ODO mg/L'},
                }
            }
        ]

        result = list(collection.aggregate(pipeline))
        if result:
            stats = result[0]
            stats.pop('_id', None)
            return jsonify(stats), 200
        else:
            return jsonify({"message": "No data"}), 404

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/outliers', methods=['GET'])
def get_outliers():

    try:
        import numpy as np
        from scipy import stats as scipy_stats

        field = request.args.get('field', 'Temperature (c)')
        method = request.args.get('method', 'iqr')
        k = float(request.args.get('k', 1.5))

        # Get data
        data = list(collection.find({field: {'$exists': True, '$ne': None}}, {field: 1, '_id': 1}))

        if not data:
            return jsonify({"message": "No data"}), 404

        values = [doc[field] for doc in data if doc.get(field) is not None]

        if method == 'iqr':
            q1 = np.percentile(values, 25)
            q3 = np.percentile(values, 75)
            iqr = q3 - q1
            lower = q1 - k * iqr
            upper = q3 + k * iqr

            outliers = []
            for doc in data:
                val = doc.get(field)
                if val is not None and (val < lower or val > upper):
                    doc['_id'] = str(doc['_id'])
                    outliers.append(doc)

        elif method == 'zscore':
            z_scores = scipy_stats.zscore(values)
            outliers = []
            for i, doc in enumerate(data):
                if abs(z_scores[i]) > k:
                    doc['_id'] = str(doc['_id'])
                    outliers.append(doc)
        else:
            return jsonify({"error": "Invalid method"}), 400

        return jsonify({
            "field": field,
            "method": method,
            "outlier_count": len(outliers),
            "outliers": outliers
        }), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, port=5001)