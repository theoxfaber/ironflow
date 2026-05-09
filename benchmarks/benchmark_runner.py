import subprocess
import time
import psutil
import os
import json
import urllib.request
import signal

def measure_dagster():
    print("--- Running Dagster Benchmark ---")
    start_time = time.time()
    
    process = subprocess.Popen(
        [".venv_heavy/bin/python", "dagster_dag.py"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )
    
    max_memory = 0
    p = psutil.Process(process.pid)
    
    while process.poll() is None:
        try:
            mem = p.memory_info().rss / (1024 * 1024)
            if mem > max_memory:
                max_memory = mem
            time.sleep(0.05)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            break
            
    process.communicate()
    end_time = time.time()
    
    if process.returncode != 0:
        print(f"Dagster failed! Return code: {process.returncode}")
        return None
        
    duration = end_time - start_time
    print(f"Dagster: {duration:.2f} seconds | Max Mem: {max_memory:.2f} MB")
    return {"duration": duration, "memory": max_memory}

def measure_airflow():
    print("--- Running Airflow Benchmark ---")
    
    env = os.environ.copy()
    env["AIRFLOW_HOME"] = os.path.abspath("airflow_home")
    env["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
    
    start_time = time.time()
    
    process = subprocess.Popen(
        [".venv_heavy/bin/airflow", "dags", "test", "benchmark_dag_100"],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )
    
    max_memory = 0
    p = psutil.Process(process.pid)
    while process.poll() is None:
        try:
            mem = p.memory_info().rss / (1024 * 1024)
            if mem > max_memory:
                max_memory = mem
            time.sleep(0.01)
        except:
            break
            
    process.communicate()
    end_time = time.time()
    
    if process.returncode != 0:
        print(f"Airflow failed! Return code: {process.returncode}")
        return None
        
    duration = end_time - start_time
    print(f"Airflow: {duration:.2f} seconds | Max Mem: {max_memory:.2f} MB")
    return {"duration": duration, "memory": max_memory}

# Keeping prefect and ironflow unchanged but reusing previous code...
def measure_prefect():
    print("--- Running Prefect Benchmark ---")
    start_time = time.time()
    process = subprocess.Popen(["../.venv/bin/python", "competitor_dag.py"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    max_memory = 0
    p = psutil.Process(process.pid)
    while process.poll() is None:
        try:
            mem = p.memory_info().rss / (1024 * 1024)
            if mem > max_memory: max_memory = mem
            time.sleep(0.01)
        except: break
    process.communicate()
    end_time = time.time()
    duration = end_time - start_time
    print(f"Prefect: {duration:.2f} seconds | Max Mem: {max_memory:.2f} MB")
    return {"duration": duration, "memory": max_memory}

def measure_ironflow():
    print("--- Running IronFlow Benchmark ---")
    db_path = "benchmark.db"
    for ext in ["", "-shm", "-wal"]:
        if os.path.exists(db_path + ext): os.remove(db_path + ext)
    os.makedirs("dags", exist_ok=True)
    os.system("cp ironflow_dag.toml dags/")
    start_time = time.time()
    server = subprocess.Popen(["../target/release/ironflow", "start", "--dags-dir", "./dags", "--db-path", f"./{db_path}", "--with-api", "--port", "8085"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(2)
    max_memory = 0
    p = psutil.Process(server.pid)
    try: max_memory = p.memory_info().rss / (1024 * 1024)
    except: pass
    urllib.request.urlopen(urllib.request.Request("http://127.0.0.1:8085/api/dags/benchmark_dag_100/trigger", method="POST"))
    completed = False
    while not completed:
        try:
            mem = p.memory_info().rss / (1024 * 1024)
            if mem > max_memory: max_memory = mem
            res = subprocess.run(["sqlite3", db_path, "SELECT status FROM dag_runs WHERE dag_id='benchmark_dag_100' ORDER BY started_at DESC LIMIT 1;"], capture_output=True, text=True)
            status = res.stdout.strip()
            if status in ["success", "failed"]: completed = True
            time.sleep(0.05)
        except: pass
    end_time = time.time()
    server.terminate()
    server.wait()
    duration = end_time - start_time - 2
    print(f"IronFlow: {duration:.2f} seconds | Max Mem: {max_memory:.2f} MB")
    return {"duration": duration, "memory": max_memory}

if __name__ == "__main__":
    subprocess.run([".venv_heavy/bin/python", "generate_dags.py"])
    print("Starting Heavyweight Benchmarks for 50 Sequential Tasks...")
    
    ironflow_metrics = measure_ironflow()
    prefect_metrics = measure_prefect()
    dagster_metrics = measure_dagster()
    airflow_metrics = measure_airflow()
    
    results = {
        "ironflow": ironflow_metrics,
        "prefect": prefect_metrics,
        "dagster": dagster_metrics,
        "airflow": airflow_metrics
    }
    with open("heavy_results.json", "w") as f:
        json.dump(results, f, indent=2)
    print("Done!")
