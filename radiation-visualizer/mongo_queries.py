# mongo_queries.py
from pymongo.collection import Collection
from typing import List, Dict, Any

def get_unique_years(collection: Collection) -> List[int]:
    unique_years_pipeline = [
        {
            "$project": {
                "year": {
                    "$year": {
                        "$dateFromString": {
                            "dateString": "$timestamp"
                        }
                    }
                }
            }
        },
        {
            "$group": {
                "_id": "$year"
            }
        }
    ]
    unique_years_result = list(collection.aggregate(unique_years_pipeline))
    return sorted([doc["_id"] for doc in unique_years_result])

def get_threshold_color(collection: Collection, threshold: float, clean_doc) -> List[Dict[str, Any]]:
    threshold_color_pipeline = [
        {
            "$project": {
                "longitude": 1,
                "latitude": 1,
                "cpm": 1,
                "micro_sv_h": 1,
                "color": {
                    "$cond": {
                        "if": {"$gt": ["$cpm", threshold]},
                        "then": [255, 0, 0],
                        "else": [0, 255, 0]
                    }
                }
            }
        }
    ]
    return [clean_doc(d) for d in collection.aggregate(threshold_color_pipeline)]

def get_heatmap_weight(collection: Collection, threshold: float, clean_doc) -> List[Dict[str, Any]]:
    heatmap_weight_pipeline = [
        {
            "$project": {
                "longitude": 1,
                "latitude": 1,
                "micro_sv_h": 1,
                "heatmap_weight": {
                    "$cond": {
                        "if": {"$gt": ["$cpm", threshold]},
                        "then": "$cpm",
                        "else": 0
                    }
                }
            }
        }
    ]
    return [clean_doc(d) for d in collection.aggregate(heatmap_weight_pipeline)]

def get_total_rows(collection: Collection, selected_year: int) -> int:
    count_pipeline = [
        {
            "$project": {
                "year": {
                    "$year": {
                        "$dateFromString": {
                            "dateString": "$timestamp"
                        }
                    }
                }
            }
        },
        {
            "$match": {
                "year": selected_year
            }
        },
        {
            "$count": "total"
        }
    ]
    count_result = list(collection.aggregate(count_pipeline))
    return count_result[0]['total'] if count_result else 0

def get_selected_year_data(collection: Collection, selected_year: int, offset: int, window_size: int, clean_doc) -> List[Dict[str, Any]]:
    selected_year_pipeline = [
        {
            "$project": {
                "latitude": 1,
                "longitude": 1,
                "cpm": 1,
                "uploaded_time": 1,
                "event_time": 1,
                "timestamp": 1,
                "year": {
                    "$year": {
                        "$dateFromString": {
                            "dateString": "$timestamp"
                        }
                    }
                }
            }
        },
        {
            "$match": {
                "year": selected_year
            }
        },
        {
            "$skip": offset
        },
        {
            "$limit": window_size
        }
    ]
    return [clean_doc(d) for d in collection.aggregate(selected_year_pipeline)]

def get_all_data(collection: Collection, clean_doc) -> List[Dict[str, Any]]:
    all_data_pipeline = [
        {
            "$project": {
                "year": {
                    "$year": {
                        "$dateFromString": {
                            "dateString": "$timestamp"
                        }
                    }
                },
                "cpm": 1,
                "latitude": 1,
                "longitude": 1,
            }
        }
    ]
    return [clean_doc(d) for d in collection.aggregate(all_data_pipeline)]
