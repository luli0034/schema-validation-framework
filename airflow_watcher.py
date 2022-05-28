import os
import time
from airflow import DAG
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from datetime import datetime, timedelta
import requests

headers = {
    # Already added when you pass json= but not when you pass data=
    # 'Content-Type': 'application/json',
}


class Handler(FileSystemEventHandler):
    def on_created(self, event):
        if event.event_type == 'created':
            print(f"file {event.src_path} created")
            jobpath='job.yaml'
            schema=event.src_path
            payload = {
                'conf': {
                    'jobpath': jobpath,
                    'schema': schema
                },
            }
            response = requests.post('http://localhost:8080/api/v1/dags/codegen/dagRuns', headers=headers, json=payload, auth=('airflow', 'airflow'))
            print(response)
    
    def on_modified(event):
        pass

    def on_deleted(event):
        pass
            

def main():
    observer = Observer()
    event_handler = FileSystemEventHandler()
    observer_path = './schema/'
    observer.schedule(Handler(), observer_path, recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    
    observer.join()

if __name__ == '__main__':
    main()
