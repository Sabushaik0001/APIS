from fastapi import FastAPI, HTTPException, Path, Query, Body
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
import os
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from datetime import datetime
import logging
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any  
import os
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient
import re
import json
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Warehouse API - RESTful", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"], 
    allow_headers=["*"], 
)

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID") or os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY") or os.getenv("AWS_SECRET_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

bedrock_client = boto3.client(
    "bedrock-runtime",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("AWS_SECRET_KEY"),
    region_name=os.getenv("AWS_REGION", "us-east-1")
)


# Load Azure credentials from environment variables
AZURE_TENANT_ID = os.getenv("AZURE_TENANT_ID")
AZURE_CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
AZURE_CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")
AZURE_STORAGE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
AZURE_CONTAINER_NAME = os.getenv("AZURE_CONTAINER_NAME")

# Create credential using service principal
credential = ClientSecretCredential(
    tenant_id=AZURE_TENANT_ID,
    client_id=AZURE_CLIENT_ID,
    client_secret=AZURE_CLIENT_SECRET
)

# Initialize Blob Service Client
account_url = f"https://{AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net"
blob_service_client = BlobServiceClient(account_url=account_url, credential=credential)


def get_connection():
    return psycopg2.connect(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        database=os.getenv("PG_DATABASE")
    )

@app.get("/api/v1/warehouses")
def get_all_warehouses():
    """Get all warehouses with their employees"""
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        warehouse_query = """
            SELECT 
                warehouse_id, 
                warehouse_name, 
                warehouse_capacity,
                warehouse_longitude,
                warehouse_latitude,
                warehouse_location
            FROM public.warehouse
            ORDER BY warehouse_id
        """
        cur.execute(warehouse_query)
        warehouse_rows = cur.fetchall()
        
        if not warehouse_rows:
            cur.close()
            conn.close()
            return {
                "status": "success",
                "total_warehouses": 0,
                "warehouses": []
            }
        
        warehouses_list = []
        
        for warehouse_row in warehouse_rows:
            warehouse_id = warehouse_row[0]
            
            emp_query = """
                SELECT 
                    e.emp_id,
                    e.warehouse_id,
                    e.emp_name,
                    e.emp_number,
                    e.role_id,
                    e.emp_facecrop,
                    r.role_name
                FROM public.wh_emp_data e
                LEFT JOIN public.wh_emp_role r ON e.role_id = r.role_id
                WHERE e.warehouse_id = %s 
                    AND e.role_id IN ('ROLE_SUP', 'ROLE_INC', 'ROLE_DEO')
                ORDER BY 
                    CASE e.role_id
                        WHEN 'ROLE_SUP' THEN 1
                        WHEN 'ROLE_INC' THEN 2
                        WHEN 'ROLE_DEO' THEN 3
                        ELSE 4
                    END,
                    e.emp_name
            """
            cur.execute(emp_query, (warehouse_id,))
            emp_rows = cur.fetchall()
            
            employees = []
            for emp_row in emp_rows:
                employee = {
                    "emp_id": emp_row[0],
                    "warehouse_id": emp_row[1],
                    "emp_name": emp_row[2],
                    "emp_number": emp_row[3],
                    "role_id": emp_row[4],
                    "emp_facecrop": emp_row[5],
                    "role_name": emp_row[6]
                }
                employees.append(employee)
            
            warehouse_data = {
                "warehouse_id": warehouse_row[0],
                "warehouse_name": warehouse_row[1],
                "warehouse_capacity": warehouse_row[2],
                "warehouse_longitude": float(warehouse_row[3]) if warehouse_row[3] else None,
                "warehouse_latitude": float(warehouse_row[4]) if warehouse_row[4] else None,
                "warehouse_location": warehouse_row[5],
                "employees": employees,
                "total_employees": len(employees)
            }
            
            warehouses_list.append(warehouse_data)
        
        cur.close()
        conn.close()
        
        return {
            "status": "success",
            "total_warehouses": len(warehouses_list),
            "warehouses": warehouses_list
        }
        
    except Exception as e:
        print(f"Error fetching all warehouses: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/api/v1/warehouses/{warehouse_id}")
def get_warehouse_by_id(
    warehouse_id: str = Path(..., description="Warehouse ID (e.g., WH001)")
):
    """Get specific warehouse details with cameras, vehicles, and employees"""
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        warehouse_query = """
            SELECT 
                warehouse_id, 
                warehouse_name, 
                warehouse_capacity,
                warehouse_longitude,
                warehouse_latitude,
                warehouse_location
            FROM public.warehouse
            WHERE warehouse_id = %s
        """
        cur.execute(warehouse_query, (warehouse_id,))
        warehouse_row = cur.fetchone()
        
        if not warehouse_row:
            cur.close()
            conn.close()
            raise HTTPException(
                status_code=404, 
                detail=f"Warehouse not found: {warehouse_id}"
            )
        
        camera_query = """
            SELECT 
                cam_id,
                cam_direction,
                camera_status,
                warehouse_id,
                stream_arn,
                hls_url,
                camera_longitude,
                camera_latitude,
                services
            FROM public.cameras
            WHERE warehouse_id = %s
            ORDER BY cam_id
        """
        cur.execute(camera_query, (warehouse_id,))
        camera_rows = cur.fetchall()
        
        cameras = []
        for cam_row in camera_rows:
            camera = {
                "cam_id": cam_row[0],
                "cam_direction": cam_row[1],
                "camera_status": cam_row[2],
                "warehouse_id": cam_row[3],
                "stream_arn": cam_row[4],
                "hls_url": cam_row[5],
                "camera_longitude": float(cam_row[6]) if cam_row[6] else None,
                "camera_latitude": float(cam_row[7]) if cam_row[7] else None,
                "services": cam_row[8]
            }
            cameras.append(camera)
        
        vehicle_query = """
            SELECT 
                v.id,
                v.warehouse_id,
                v.number_plate,
                v.bags_capacity,
                v.vehicle_access,
                v.driver_id,
                v.created_at,
                d.driver_name,
                d.driver_phone,
                d.driver_crop
            FROM public.wh_vehicles v
            LEFT JOIN public.wh_drivers d ON v.driver_id = d.driver_id
            WHERE v.warehouse_id = %s
            ORDER BY v.id
        """
        cur.execute(vehicle_query, (warehouse_id,))
        vehicle_rows = cur.fetchall()
        
        vehicles = []
        for veh_row in vehicle_rows:
            vehicle = {
                "id": veh_row[0],
                "warehouse_id": veh_row[1],
                "number_plate": veh_row[2],
                "bags_capacity": veh_row[3],
                "vehicle_access": veh_row[4],
                "driver_id": veh_row[5],
                "created_at": veh_row[6].strftime('%Y-%m-%d %H:%M:%S') if veh_row[6] else None,
                "driver_name": veh_row[7],
                "driver_phone": veh_row[8],
                "driver_crop": veh_row[9]
            }
            vehicles.append(vehicle)
        
        emp_query = """
            SELECT 
                e.emp_id,
                e.warehouse_id,
                e.emp_name,
                e.emp_number,
                e.role_id,
                e.emp_facecrop,
                r.role_name
            FROM public.wh_emp_data e
            LEFT JOIN public.wh_emp_role r ON e.role_id = r.role_id
            WHERE e.warehouse_id = %s
            ORDER BY e.role_id, e.emp_name
        """
        cur.execute(emp_query, (warehouse_id,))
        emp_rows = cur.fetchall()
        
        employees = []
        for emp_row in emp_rows:
            employee = {
                "emp_id": emp_row[0],
                "warehouse_id": emp_row[1],
                "emp_name": emp_row[2],
                "emp_number": emp_row[3],
                "role_id": emp_row[4],
                "emp_facecrop": emp_row[5],
                "role_name": emp_row[6]
            }
            employees.append(employee)
        
        warehouse_data = {
            "warehouse_id": warehouse_row[0],
            "warehouse_name": warehouse_row[1],
            "warehouse_capacity": warehouse_row[2],
            "warehouse_longitude": float(warehouse_row[3]) if warehouse_row[3] else None,
            "warehouse_latitude": float(warehouse_row[4]) if warehouse_row[4] else None,
            "warehouse_location": warehouse_row[5]
        }
        
        cur.close()
        conn.close()
        
        return {
            "status": "success",
            "warehouse": warehouse_data,
            "cameras": {
                "total_cameras": len(cameras),
                "data": cameras
            },
            "vehicles": {
                "total_vehicles": len(vehicles),
                "data": vehicles
            },
            "employees": {
                "total_employees": len(employees),
                "data": employees
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error fetching warehouse by ID: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/")
@app.get("/health")
def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "message": "Warehouse API is running",
        "version": "1.0.0",
        "endpoints": {
            "warehouses": "GET /api/v1/warehouses - Get all warehouses with employees",
            "warehouse_by_id": "GET /api/v1/warehouses/{warehouse_id} - Get specific warehouse details",
            "camera_stream": "GET /api/v1/cameras/stream-url - Get HLS streaming URL for camera",
            "chunks": "GET /api/v1/warehouses/{warehouse_id}/cameras/{cam_id}/chunks - Get video chunks",
            "employee_logs": "GET /api/v1/warehouses/{warehouse_id}/cameras/{cam_id}/logs/employees - Get employee logs",
            "gunny_logs": "GET /api/v1/warehouses/{warehouse_id}/cameras/{cam_id}/logs/gunny-bags - Get gunny bag logs",
            "vehicle_logs": "GET /api/v1/warehouses/{warehouse_id}/cameras/{cam_id}/logs/vehicles - Get vehicle logs",
            "dashboard": "GET /api/v1/warehouses/{warehouse_id}/dashboard - Get dashboard analytics",
            "vehicle_gunny_analytics": "GET /api/v1/warehouses/{warehouse_id}/cameras/{cam_id}/analytics/vehicle-gunny-count - Get vehicle-wise gunny count"
        }
    }

@app.get("/api/v1/cameras/stream-url")
def get_camera_stream_url(
    warehouse_id: str = Query(..., description="Warehouse ID (e.g., WH001)"),
    cam_id: str = Query(..., description="Camera ID")
):
    """Get HLS streaming URL for a specific camera"""
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        camera_query = """
            SELECT 
                cam_id,
                warehouse_id,
                stream_arn,
                hls_url,
                cam_direction,
                camera_status
            FROM public.cameras
            WHERE warehouse_id = %s AND cam_id = %s
        """
        cur.execute(camera_query, (warehouse_id, cam_id))
        camera_row = cur.fetchone()
        
        if not camera_row:
            cur.close()
            conn.close()
            raise HTTPException(
                status_code=404,
                detail=f"Camera not found: cam_id={cam_id}, warehouse_id={warehouse_id}"
            )
        
        stream_arn = camera_row[2]
        
        if not stream_arn:
            cur.close()
            conn.close()
            raise HTTPException(
                status_code=400,
                detail=f"Stream ARN not configured for camera: {cam_id}"
            )
        
        try:
            stream_name = stream_arn.split('/')[1]
        except IndexError:
            cur.close()
            conn.close()
            raise HTTPException(
                status_code=400,
                detail="Invalid stream ARN format in database"
            )
        
        kvs_client = boto3.client(
            'kinesisvideo',
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name=AWS_REGION
        )
        
        endpoint_response = kvs_client.get_data_endpoint(
            StreamARN=stream_arn,
            APIName='GET_HLS_STREAMING_SESSION_URL'
        )
        
        data_endpoint = endpoint_response['DataEndpoint']
        
        kvs_archived_media_client = boto3.client(
            'kinesis-video-archived-media',
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name=AWS_REGION,
            endpoint_url=data_endpoint
        )
        
        expires = 3600 
        hls_response = kvs_archived_media_client.get_hls_streaming_session_url(
            StreamARN=stream_arn,
            PlaybackMode='LIVE',
            HLSFragmentSelector={
                'FragmentSelectorType': 'SERVER_TIMESTAMP'
            },
            Expires=expires
        )
        
        hls_url = hls_response['HLSStreamingSessionURL']
        
        update_query = """
            UPDATE public.cameras
            SET camera_status = 'active', hls_url = %s
            WHERE warehouse_id = %s AND cam_id = %s
        """
        cur.execute(update_query, (hls_url, warehouse_id, cam_id))
        conn.commit()
        
        rows_updated = cur.rowcount
        cur.close()
        conn.close()
        
        update_status = "Camera status updated to 'active' and HLS URL saved" if rows_updated > 0 else "Camera update failed"
        
        return {
            "status": "success",
            "stream_arn": stream_arn,
            "stream_name": stream_name,
            "warehouse_id": warehouse_id,
            "cam_id": cam_id,
            "hls_streaming_url": hls_url,
            "expires_in_seconds": expires,
            "data_endpoint": data_endpoint,
            "database_update": update_status
        }
        
    except HTTPException:
        raise
    except ClientError as e:
        if conn:
            conn.close()
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        logger.error(f"AWS Error: {error_code} - {error_message}")
        raise HTTPException(
            status_code=400,
            detail=f"AWS Kinesis Error: {error_code} - {error_message}"
        )
    except Exception as e:
        if conn:
            conn.close()
        logger.error(f"Error getting HLS URL: {e}")
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@app.get("/api/v1/warehouses/{warehouse_id}/cameras/{cam_id}/chunks")
def get_camera_chunks(
    warehouse_id: str = Path(..., description="Warehouse ID (e.g., WH001)"),
    cam_id: str = Path(..., description="Camera ID"),
    date: str = Query(..., description="Date in YYYY-MM-DD format")
):
    """Get video chunks for a specific camera and date"""
    try:
        try:
            datetime.strptime(date, '%Y-%m-%d')
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="Invalid date format. Use YYYY-MM-DD"
            )
        
        conn = get_connection()
        cur = conn.cursor()
        
        chunks_query = """
            SELECT 
                chunk_id,
                warehouse_id,
                cam_id,
                chunk_blob_url,
                transcripts_url,
                date,
                time
            FROM public.wh_chunks
            WHERE warehouse_id = %s AND cam_id = %s AND date = %s
            ORDER BY time
        """
        cur.execute(chunks_query, (warehouse_id, cam_id, date))
        chunk_rows = cur.fetchall()
        
        if not chunk_rows:
            cur.close()
            conn.close()
            return {
                "status": "success",
                "message": "No chunks found for the given criteria",
                "warehouse_id": warehouse_id,
                "cam_id": cam_id,
                "date": date,
                "total_chunks": 0,
                "chunks": []
            }
        
        chunks = []
        for chunk_row in chunk_rows:
            chunk = {
                "chunk_id": chunk_row[0],
                "warehouse_id": chunk_row[1],
                "cam_id": chunk_row[2],
                "chunk_blob_url": chunk_row[3],
                "transcripts_url": chunk_row[4],
                "date": chunk_row[5].strftime('%Y-%m-%d') if chunk_row[5] else None,
                "time": chunk_row[6].strftime('%Y-%m-%d %H:%M:%S') if chunk_row[6] else None
            }
            chunks.append(chunk)
        
        cur.close()
        conn.close()
        
        return {
            "status": "success",
            "warehouse_id": warehouse_id,
            "cam_id": cam_id,
            "date": date,
            "total_chunks": len(chunks),
            "chunks": chunks
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching chunks: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/api/v1/warehouses/{warehouse_id}/cameras/{cam_id}/logs/employees")
def get_employee_logs(
    warehouse_id: str = Path(..., description="Warehouse ID (e.g., WH001)"),
    cam_id: str = Path(..., description="Camera ID"),
    date: str = Query(..., description="Date in YYYY-MM-DD format")
):
    """Get employee logs for a specific camera and date"""
    try:
        try:
            datetime.strptime(date, '%Y-%m-%d')
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="Invalid date format. Use YYYY-MM-DD"
            )
        
        conn = get_connection()
        cur = conn.cursor()
        
        emp_logs_query = """
            SELECT 
                el.id,
                el.warehouse_id,
                el.emp_id,
                e.emp_name,
                e.emp_number,
                r.role_name,
                el.date,
                el.time,
                el.cam_id,
                el.crop_blob_url,
                el.chunk_id,
                el.emp_access
            FROM public.wh_emp_logs el
            LEFT JOIN public.wh_emp_data e ON el.emp_id = e.emp_id
            LEFT JOIN public.wh_emp_role r ON e.role_id = r.role_id
            WHERE el.warehouse_id = %s AND el.cam_id = %s AND el.date = %s
            ORDER BY el.time
        """
        cur.execute(emp_logs_query, (warehouse_id, cam_id, date))
        log_rows = cur.fetchall()
        
        if not log_rows:
            cur.close()
            conn.close()
            return {
                "status": "success",
                "message": "No employee logs found for the given criteria",
                "warehouse_id": warehouse_id,
                "cam_id": cam_id,
                "date": date,
                "total_logs": 0,
                "hourly_ranges": []
            }
        
        from collections import defaultdict
        hourly_logs = defaultdict(list)
        
        for row in log_rows:
            log_time = row[7] 
            if log_time:
                hour = log_time.hour
                log_entry = {
                    "log_id": row[0],
                    "warehouse_id": row[1],
                    "emp_id": row[2],
                    "emp_name": row[3],
                    "emp_number": row[4],
                    "role_name": row[5],
                    "date": row[6].strftime('%Y-%m-%d') if row[6] else None,
                    "time": log_time.strftime('%Y-%m-%d %H:%M:%S'),
                    "cam_id": row[8],
                    "crop_blob_url": row[9],
                    "chunk_id": row[10],
                    "emp_access": row[11]
                }
                hourly_logs[hour].append(log_entry)
        
        hourly_ranges = []
        for hour in sorted(hourly_logs.keys()):
            hourly_ranges.append({
                "hour_range": f"{hour:02d}:00 - {hour:02d}:59",
                "start_time": f"{hour:02d}:00",
                "end_time": f"{hour:02d}:59",
                "total_logs": len(hourly_logs[hour]),
                "unique_employees": len(set(log["emp_id"] for log in hourly_logs[hour] if log["emp_id"])),
                "logs": hourly_logs[hour]
            })
        
        cur.close()
        conn.close()
        
        return {
            "status": "success",
            "warehouse_id": warehouse_id,
            "cam_id": cam_id,
            "date": date,
            "total_logs": len(log_rows),
            "unique_employees": len(set(row[2] for row in log_rows if row[2])),
            "hourly_ranges": hourly_ranges
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching employee logs: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/api/v1/warehouses/{warehouse_id}/cameras/{cam_id}/logs/gunny-bags")
def get_gunny_bag_logs(
    warehouse_id: str = Path(..., description="Warehouse ID (e.g., WH001)"),
    cam_id: str = Path(..., description="Camera ID"),
    date: str = Query(..., description="Date in YYYY-MM-DD format")
):
    """Get gunny bag logs for a specific camera and date"""
    try:
        try:
            datetime.strptime(date, '%Y-%m-%d')
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="Invalid date format. Use YYYY-MM-DD"
            )
        
        conn = get_connection()
        cur = conn.cursor()
        
        gunny_logs_query = """
            SELECT 
                id,
                warehouse_id,
                cam_id,
                count,
                date,
                chunk_id,
                created_at,
                action
                
            FROM public.wh_gunny_logs
            WHERE warehouse_id = %s AND cam_id = %s AND date = %s
            ORDER BY created_at
        """
        cur.execute(gunny_logs_query, (warehouse_id, cam_id, date))
        log_rows = cur.fetchall()
        
        if not log_rows:
            cur.close()
            conn.close()
            return {
                "status": "success",
                "message": "No gunny bag logs found for the given criteria",
                "warehouse_id": warehouse_id,
                "cam_id": cam_id,
                "date": date,
                "total_logs": 0,
                "total_bags": 0,
                "logs": []
            }
        
        logs = []
        total_bags = 0
        action_summary = {}
        
        for row in log_rows:
            bag_count = row[3] or 0
            action = row[7]  # Changed from row[8] to row[7]
            
            log_entry = {
                "log_id": row[0],
                "warehouse_id": row[1],
                "cam_id": row[2],
                "count": bag_count,
                "date": row[4].strftime('%Y-%m-%d') if row[4] else None,
                "chunk_id": row[5],
                "created_at": row[6].strftime('%H:%M:%S') if row[6] else None,
                "action": action
            }
            logs.append(log_entry)
            
            total_bags += bag_count
            
            if action:
                if action not in action_summary:
                    action_summary[action] = {"count": 0, "total_bags": 0}
                action_summary[action]["count"] += 1
                action_summary[action]["total_bags"] += bag_count
        
        cur.close()
        conn.close()
        
        return {
            "status": "success",
            "warehouse_id": warehouse_id,
            "cam_id": cam_id,
            "date": date,
            "total_logs": len(logs),
            "total_bags": total_bags,
            "action_summary": action_summary,
            "logs": logs
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching gunny bag logs: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
@app.get("/api/v1/warehouses/{warehouse_id}/cameras/{cam_id}/logs/vehicles")
def get_vehicle_logs(
    warehouse_id: str = Path(..., description="Warehouse ID (e.g., WH001)"),
    cam_id: str = Path(..., description="Camera ID"),
    date: str = Query(..., description="Date in YYYY-MM-DD format")
):
    """Get vehicle logs for a specific camera and date"""
    try:
        try:
            datetime.strptime(date, '%Y-%m-%d')
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="Invalid date format. Use YYYY-MM-DD"
            )
        
        conn = get_connection()
        cur = conn.cursor()
        
        vehicle_logs_query = """
            SELECT 
                id,
                warehouse_id,
                cam_id,
                date,
                chunk_id,
                number_plate,
                vehicle_access,
                created_at
            FROM public.wh_vehicle_logs
            WHERE warehouse_id = %s AND cam_id = %s AND date = %s
            ORDER BY created_at
        """
        cur.execute(vehicle_logs_query, (warehouse_id, cam_id, date))
        log_rows = cur.fetchall()
        
        if not log_rows:
            cur.close()
            conn.close()
            return {
                "status": "success",
                "message": "No vehicle logs found for the given criteria",
                "warehouse_id": warehouse_id,
                "cam_id": cam_id,
                "date": date,
                "total_logs": 0,
                "logs": []
            }
        
        logs = []
        unique_vehicles = set()
        access_summary = {}
        
        for row in log_rows:
            number_plate = row[5]
            vehicle_access = row[6]
            
            log_entry = {
                "log_id": row[0],
                "warehouse_id": row[1],
                "cam_id": row[2],
                "date": row[3].strftime('%Y-%m-%d') if row[3] else None,
                "chunk_id": row[4],
                "number_plate": number_plate,
                "vehicle_access": vehicle_access,
                "created_at": row[7].strftime('%H:%M:%S') if row[7] else None
            }
            logs.append(log_entry)
            
            if number_plate:
                unique_vehicles.add(number_plate)
            
            if vehicle_access:
                if vehicle_access not in access_summary:
                    access_summary[vehicle_access] = 0
                access_summary[vehicle_access] += 1
        
        cur.close()
        conn.close()
        
        return {
            "status": "success",
            "warehouse_id": warehouse_id,
            "cam_id": cam_id,
            "date": date,
            "total_logs": len(logs),
            "unique_vehicles": len(unique_vehicles),
            "access_summary": access_summary,
            "logs": logs
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching vehicle logs: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")



@app.get("/api/v1/warehouses/{warehouse_id}/dashboard")
def get_warehouse_dashboard(
    warehouse_id: str = Path(..., description="Warehouse ID (e.g., WH001)"),
    date: str = Query(..., description="Date in YYYY-MM-DD format")
):
    """Get dashboard analytics for a specific warehouse and date"""
    try:
        try:
            datetime.strptime(date, '%Y-%m-%d')
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="Invalid date format. Use YYYY-MM-DD"
            )

        conn = get_connection()
        cur = conn.cursor()

        # bags (unchanged)
        bags_query = """
            SELECT 
                COALESCE(SUM(CASE WHEN LOWER(action) = 'loading' THEN count ELSE 0 END), 0) as loaded_bags,
                COALESCE(SUM(CASE WHEN LOWER(action) = 'unloading' THEN count ELSE 0 END), 0) as unloaded_bags
            FROM public.wh_gunny_logs
            WHERE warehouse_id = %s AND date = %s
        """
        cur.execute(bags_query, (warehouse_id, date))
        bags_result = cur.fetchone()

        # vehicles (unchanged)
        vehicles_query = """
            SELECT 
                COUNT(DISTINCT CASE 
                    WHEN LOWER(vehicle_access) IN ('authorized', 'authorised') 
                    THEN number_plate 
                END) as authorised_vehicles,
                COUNT(DISTINCT CASE 
                    WHEN LOWER(vehicle_access) IN ('unauthorized', 'unauthorised') 
                    THEN number_plate 
                END) as unauthorised_vehicles
            FROM public.wh_vehicle_logs
            WHERE warehouse_id = %s AND date = %s
        """
        cur.execute(vehicles_query, (warehouse_id, date))
        vehicles_result = cur.fetchone()

        # employee summary (new)
        emp_summary_query = """
            SELECT
                COUNT(*) AS total_employee_logs,
                COUNT(DISTINCT emp_id) FILTER (WHERE emp_id IS NOT NULL) AS total_unique_authorised_employees,
                COUNT(*) FILTER (WHERE emp_id IS NULL) AS total_unauthorised_entries
            FROM public.wh_emp_logs
            WHERE warehouse_id = %s AND date = %s
        """
        cur.execute(emp_summary_query, (warehouse_id, date))
        emp_summary = cur.fetchone()

        cur.close()
        conn.close()

        # safe defaults if None
        total_loaded_bags = bags_result[0] if bags_result and bags_result[0] is not None else 0
        total_unloaded_bags = bags_result[1] if bags_result and bags_result[1] is not None else 0
        total_authorised_vehicles = vehicles_result[0] if vehicles_result and vehicles_result[0] is not None else 0
        total_unauthorised_vehicles = vehicles_result[1] if vehicles_result and vehicles_result[1] is not None else 0

        total_employee_logs = emp_summary[0] if emp_summary and emp_summary[0] is not None else 0
        total_unique_authorised_employees = emp_summary[1] if emp_summary and emp_summary[1] is not None else 0
        total_unauthorised_entries = emp_summary[2] if emp_summary and emp_summary[2] is not None else 0

        return {
            "status": "success",
            "warehouse_id": warehouse_id,
            "date": date,
            "total_loaded_bags": total_loaded_bags,
            "total_unloaded_bags": total_unloaded_bags,
            "total_authorised_vehicles": total_authorised_vehicles,
            "total_unauthorised_vehicles": total_unauthorised_vehicles,
            # employee stats
            "total_employee_logs": total_employee_logs,
            "total_unique_authorised_employees": total_unique_authorised_employees,
            "total_unauthorised_entries": total_unauthorised_entries
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching dashboard data: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    
@app.get("/api/v1/warehouses/{warehouse_id}/cameras/{cam_id}/analytics/vehicle-gunny-count")
def get_vehicle_wise_gunny_count(
    warehouse_id: str = Path(..., description="Warehouse ID (e.g., WH001)"),
    cam_id: str = Path(..., description="Camera ID"),
    date: str = Query(..., description="Date in YYYY-MM-DD format")
):
    """Get vehicle-wise gunny bag count analytics"""
    try:
        # Validate date format
        try:
            datetime.strptime(date, '%Y-%m-%d')
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="Invalid date format. Use YYYY-MM-DD"
            )
        
        conn = get_connection()
        cur = conn.cursor()
        
        # Step 1: Get unique vehicles and their chunk_ids from vehicle_logs
        vehicle_logs_query = """
            SELECT 
                number_plate,
                ARRAY_AGG(DISTINCT chunk_id) as chunk_ids
            FROM public.wh_vehicle_logs
            WHERE warehouse_id = %s 
                AND cam_id = %s 
                AND date = %s 
                AND number_plate IS NOT NULL
                AND chunk_id IS NOT NULL
            GROUP BY number_plate
            ORDER BY number_plate
        """
        cur.execute(vehicle_logs_query, (warehouse_id, cam_id, date))
        vehicle_rows = cur.fetchall()
        
        if not vehicle_rows:
            cur.close()
            conn.close()
            return {
                "status": "success",
                "message": "No vehicles found for the given criteria",
                "warehouse_id": warehouse_id,
                "cam_id": cam_id,
                "date": date,
                "total_vehicles": 0,
                "grand_total_bags": 0,
                "vehicles": []
            }
        
        # Step 2: For each vehicle, get gunny counts from gunny_logs based on chunk_ids
        vehicles = []
        grand_total_bags = 0
        
        for row in vehicle_rows:
            number_plate = row[0]
            chunk_ids = row[1]  # This is already an array from PostgreSQL
            
            # Query gunny_logs for this vehicle's chunks
            gunny_query = """
                SELECT 
                    action,
                    SUM(count) as total_count,
                    COUNT(*) as entry_count,
                    MIN(created_at) as first_entry_time,
                    MAX(created_at) as last_entry_time
                FROM public.wh_gunny_logs
                WHERE warehouse_id = %s 
                    AND cam_id = %s 
                    AND date = %s 
                    AND chunk_id = ANY(%s)
                GROUP BY action
                ORDER BY action
            """
            cur.execute(gunny_query, (warehouse_id, cam_id, date, chunk_ids))
            gunny_rows = cur.fetchall()
            
            action_breakdown = []
            total_bags_all_actions = 0
            
            for gunny_row in gunny_rows:
                action = gunny_row[0]
                total_count = gunny_row[1] or 0
                entry_count = gunny_row[2]
                first_entry = gunny_row[3]
                last_entry = gunny_row[4]
                
                action_breakdown.append({
                    "action": action,
                    "total_count": total_count,
                    "number_of_entries": entry_count,
                    "first_entry_time": first_entry.strftime('%H:%M:%S') if first_entry else None,
                    "last_entry_time": last_entry.strftime('%H:%M:%S') if last_entry else None
                })
                
                total_bags_all_actions += total_count
            
            vehicle_entry = {
                "number_plate": number_plate,
                "chunk_ids": chunk_ids,
                "total_bags_all_actions": total_bags_all_actions,
                "action_breakdown": action_breakdown
            }
            
            vehicles.append(vehicle_entry)
            grand_total_bags += total_bags_all_actions
        
        cur.close()
        conn.close()
        
        return {
            "status": "success",
            "warehouse_id": warehouse_id,
            "cam_id": cam_id,
            "date": date,
            "total_vehicles": len(vehicles),
            "grand_total_bags": grand_total_bags,
            "vehicles": vehicles
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching vehicle-wise gunny count: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")




#Chat_API Endpoints are below 

# ==========================================
# Pydantic Models (Same as before)
# ==========================================
class MessageContent(BaseModel):
    text: str

class ConversationMessage(BaseModel):
    role: str = Field(..., description="Role: 'user' or 'assistant'")
    content: List[MessageContent]

class InferenceConfig(BaseModel):
    maxTokens: Optional[int] = Field(1000, description="Maximum tokens in response")
    temperature: Optional[float] = Field(0.7, description="Temperature for response randomness")
    topP: Optional[float] = Field(0.9, description="Top P sampling parameter")

class ChatRequest(BaseModel):
    UserQuery: str = Field(..., description="User's question about the video")
    modelId: str = Field("anthropic.claude-3-5-haiku-20241022-v1:0", description="Bedrock model ID")
    conversation: Optional[List[ConversationMessage]] = Field([], description="Previous conversation history")
    inferenceConfig: Optional[InferenceConfig] = Field(default_factory=InferenceConfig)
    chatTransactionId: Optional[str] = Field(None, description="Transaction ID for tracking")

class ChatResponse(BaseModel):
    conversation: List[ConversationMessage]
    chatLastTime: str
    chatTransactionId: str
    modelId: str
    inferenceConfig: InferenceConfig

# ==========================================
# Transcript Processing Functions (Updated for Blob)
# ==========================================

def list_transcript_files(container_name: str, prefix: str) -> List[str]:
    """List all transcript JSON files in Azure Blob with chunk_start in name"""
    try:
        container_client = blob_service_client.get_container_client(container_name)
        blob_list = container_client.list_blobs(name_starts_with=prefix)
        
        return [
            blob.name for blob in blob_list
            if blob.name.endswith('.json') and 'chunk_start' in blob.name
        ]
    except Exception as e:
        logger.error(f"Error listing transcript files: {e}")
        return []

def extract_chunk_start(blob_name: str) -> int:
    """Extract chunk start number from blob name"""
    match = re.search(r'chunk_start-(\d+)', blob_name)
    return int(match.group(1)) if match else float('inf')

def merge_transcripts(container_name: str, blob_names: List[str]) -> Dict[str, Any]:
    """Merge multiple transcript JSON files into single result"""
    results = []
    sorted_names = sorted(blob_names, key=extract_chunk_start)
    container_client = blob_service_client.get_container_client(container_name)
    
    for blob_name in sorted_names:
        try:
            blob_client = container_client.get_blob_client(blob_name)
            blob_data = blob_client.download_blob()
            content = blob_data.readall().decode('utf-8')
            data = json.loads(content)
            
            if isinstance(data, list):
                results.extend(data)
            else:
                results.append(data)
        except json.JSONDecodeError as e:
            logger.warning(f"Skipping invalid JSON in {blob_name}: {e}")
        except Exception as e:
            logger.error(f"Error reading {blob_name}: {e}")
    
    return {
        "statusCode": 200,
        "videoTranscript": {
            "results": json.dumps(results),
            "count_results": []
        }
    }

def build_video_context(transcript_data: Dict[str, Any]) -> str:
    """Build video context string from transcript data (Same as before)"""
    video_context = ""
    
    try:
        if "videoTranscript" in transcript_data and "results" in transcript_data["videoTranscript"]:
            results = transcript_data["videoTranscript"]["results"]
            
            if isinstance(results, str):
                results = json.loads(results)
            
            for item in results:
                if isinstance(item, dict):
                    for key in sorted(item):
                        video_context += f"**************{key}**************\n"
                        video_context += f"{item[key]}\n\n"
    except Exception as e:
        logger.error(f"Error parsing transcript: {e}")
        raise
    
    return video_context

# ==========================================
# System Prompt Template (Same as before)
# ==========================================

SYSTEM_TEMPLATE = """The following is a friendly conversation between a Human (H) and an AI Assistant (AI) about a Video. There is no video provided to you but only a transcript of the video. Always remember the following points when having a conversation,

- The Video information is provided to you in the `Video Context` section below. You are to only answer based on the <video_context>...</video_context> and if the answer is not available respond with "I don't know, I'm sorry the requested information is not a part of the video".

- The video transcript is a non-overlapping second by second summary provided by a video transcriber. You are to answer a user's question based on the entire transcript and keep the user's conversation history in context when answering the question.

- Remember when a human asks about a video, always assume they are talking about the <video_context>...</video_context> transcript and respond appropriately. Your job depends on this.

- The user does not know that you (the assistant) has the video context. You should never reveal this information back to the user. Your job is to make them think that you analyzing the video live. It's your secret to never talk about <video_context>...</video_context>.

- Remember never reveal to the user about video context. Always pretend that you have access to the video.

- The video context is your biggest secret. Your job depends on this.

<video_context>
{video_context}
</video_context>
"""

# ==========================================
# Chat Endpoint (Updated for Blob)
# ==========================================

@app.post(
    "/api/v1/warehouses/{warehouse_id}/cameras/{cam_id}/chunks/{chunk_id}/chat",
    response_model=ChatResponse,
    summary="Chat with AI about video chunk",
    description="Ask questions about a specific video chunk using AI assistant"
)
async def chat_with_video(
    warehouse_id: str = Path(..., description="Warehouse ID (e.g., WH001)"),
    cam_id: str = Path(..., description="Camera ID"),
    chunk_id: str = Path(..., description="Chunk ID (e.g., chunk_2025-01-15_10-00-00)"),
    request: ChatRequest = Body(...)
):
    """
    Chat endpoint for asking questions about video content
    
    This endpoint:
    1. Fetches chunk transcript URL from database
    2. Retrieves and merges video transcripts from Azure Blob Storage
    3. Builds context from transcript
    4. Sends query to AWS Bedrock AI with conversation history
    5. Returns AI response with updated conversation
    """
    
    try:
        logger.info(f"Chat request for warehouse={warehouse_id}, camera={cam_id}, chunk={chunk_id}")
        
        # Step 1: Get chunk transcript URL from database
        conn = get_connection()
        cur = conn.cursor()
        
        chunk_query = """
            SELECT 
                chunk_id,
                warehouse_id,
                cam_id,
                chunk_blob_url,
                transcripts_url,
                date,
                time
            FROM public.wh_chunks
            WHERE warehouse_id = %s AND cam_id = %s AND chunk_id = %s
        """
        cur.execute(chunk_query, (warehouse_id, cam_id, chunk_id))
        chunk_row = cur.fetchone()
        
        if not chunk_row:
            cur.close()
            conn.close()
            raise HTTPException(
                status_code=404,
                detail=f"Chunk not found: warehouse_id={warehouse_id}, cam_id={cam_id}, chunk_id={chunk_id}"
            )
        
        # transcript_blob_url = chunk_row[4]  # transcripts_url column
        transcript_blob_url = "https://spectradevdev.blob.core.windows.net/cache-0e83775c98f1d6627efbe49f1ca0ba9b-eastus/2025-08-26/loopcam1/10028814-d9e1-4c85-8a7d-74e034381b4d/chunks/ts_10028814-d9e1-4c85-8a7d-74e034381b4d_chunk_start-0-end-30_file.json"
        if not transcript_blob_url:
            cur.close()
            conn.close()
            raise HTTPException(
                status_code=400,
                detail=f"No transcript URL configured for chunk {chunk_id}"
            )
        
        cur.close()
        conn.close()
        
        # Step 2: Parse Blob URL
        # Example: https://account.blob.core.windows.net/container/transcripts/chunk_2025-01-15_10-00-00/
        # OR: wasbs://container@account.blob.core.windows.net/transcripts/chunk_2025-01-15_10-00-00/
        
        if transcript_blob_url.startswith("https://"):
            url_parts = transcript_blob_url.replace("https://", "").split("/")
            container_name = url_parts[1]
            # Remove file name if present and get only the folder as prefix
            last_part = url_parts[-1]
            print("the last part\n",last_part)
            if last_part.endswith('.json'):
                blob_prefix = "/".join(url_parts[2:-1]) + "/"  # Get only containing folder
            else:
                blob_prefix = "/".join(url_parts[2:]) + "/"    # already a folder
        else:
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported blob URL format: {transcript_blob_url}"
            )
        
        # Ensure prefix ends with / for proper folder listing
        if not blob_prefix.endswith("/"):
            blob_prefix += "/"
        
        logger.info(f"Looking for transcripts in container: {container_name}, prefix: {blob_prefix}")
        
        # Step 3: List and merge transcript files
        transcript_blobs = list_transcript_files(container_name, blob_prefix)
        print("=="*130)
        print("the transcripts blobs\n",transcript_blobs)
        print("=="*130)
        if not transcript_blobs:
            raise HTTPException(
                status_code=404,
                detail=f"No transcript files found for chunk_id={chunk_id}"
            )
        
        transcript_data = merge_transcripts(container_name, transcript_blobs)
        logger.info(f"Merged {len(transcript_blobs)} transcript files")
        
        # Step 4: Build video context
        video_context = build_video_context(transcript_data)
        
        if not video_context:
            raise HTTPException(
                status_code=500,
                detail="Failed to build video context from transcripts"
            )
        
        # Step 5: Prepare system prompt and messages
        system_prompt = SYSTEM_TEMPLATE.replace("{video_context}", video_context)
        system_list = [{"text": system_prompt}]
        
        # Build message list with conversation history
        message_list = []
        if request.conversation:
            # Convert Pydantic models to dicts for Bedrock
            message_list = [
                {
                    "role": msg.role,
                    "content": [{"text": c.text} for c in msg.content]
                }
                for msg in request.conversation
            ]
        
        # Add current user query
        message_list.append({
            "role": "user",
            "content": [{"text": request.UserQuery}]
        })
        
        # Step 6: Call AWS Bedrock (Same as before)
        logger.info(f"Calling Bedrock model: {request.modelId}")
        
        inference_config = {
            "maxTokens": request.inferenceConfig.maxTokens,
            "temperature": request.inferenceConfig.temperature,
            "topP": request.inferenceConfig.topP
        }
        # Ideally you pass the ARN from frontend or fetch it programmatically
        inference_profile_arn = "arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-3-5-haiku-20241022-v1:0"

        bedrock_response = bedrock_client.converse(
            modelId=inference_profile_arn,  # <-- changed
            messages=message_list,
            system=system_list,
            inferenceConfig=inference_config
        )

        bedrock_response = bedrock_client.converse(
            modelId=request.modelId,
            messages=message_list,
            system=system_list,
            inferenceConfig=inference_config
        )
        
        # Step 7: Extract assistant response
        if not (bedrock_response and 'output' in bedrock_response and 
                'message' in bedrock_response['output']):
            raise HTTPException(
                status_code=500,
                detail="No response from AI model"
            )
        
        assistant_text = bedrock_response['output']['message']['content'][0]['text']
        logger.info(f"Assistant response received: {len(assistant_text)} characters")
        
        # Step 8: Add assistant response to conversation
        message_list.append({
            "role": "assistant",
            "content": [{"text": assistant_text}]
        })
        
        # Step 9: Build response
        chat_transaction_id = request.chatTransactionId or str(uuid.uuid4().hex)
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Convert back to Pydantic models for response
        conversation_response = [
            ConversationMessage(
                role=msg["role"],
                content=[MessageContent(text=c["text"]) for c in msg["content"]]
            )
            for msg in message_list
        ]
        
        return ChatResponse(
            conversation=conversation_response,
            chatLastTime=current_time,
            chatTransactionId=chat_transaction_id,
            modelId=request.modelId,
            inferenceConfig=request.inferenceConfig
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in chat endpoint: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )

# ==========================================
# CLI Support (for direct execution)
# ==========================================

if __name__ == "__main__":
    import time
    import uvicorn
    
    # Check if running with --api flag for FastAPI server
    import sys
    
    if "--api" in sys.argv:
        # Run as FastAPI server
        port = 8081
        host = "20.84.162.92"
        logger.info(f"Starting FastAPI server on {host}:{port}")
        logger.info(f"API available at http://{host}:{port}/")
        uvicorn.run(app, host=host, port=port, reload=True, log_level=LOG_LEVEL.lower())
    else:
        # Run as CLI script (original behavior)
        # Example payload - CHANGE THESE VALUES
        warehouse_id = "WH001"
        camera_id = 1
        date = "22-09-2025"  # Can also use format "2025-09-22"
        
        start_time = time.time()
        result = get_warehouse_data_with_sessions(warehouse_id, camera_id, date)
        end_time = time.time()
        
        elapsed_time = end_time - start_time
        
        # Print summary statistics
        print_summary_stats(result)