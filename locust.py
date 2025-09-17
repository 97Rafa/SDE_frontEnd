from locust import HttpUser, task, between
import random, string
import json

class SDEUser(HttpUser):
    host = "http://localhost:4000"
    # Wait time between tasks (to simulate real users)
    wait_time = between(1, 5)

    # # Example: Produce request to Kafka
    # @task
    # def produce_request(self):
    #     payload = {"msg": f"test-{random.randint(1,1000)}"}
    #     topic = "request_topic"   # adjust with settings.req_topic
    #     self.client.post(f"/produce/{topic}", json=payload)

    # # Example: Consume requests
    # @task
    # def consume_requests(self):
    #     topic = "request_topic"   # adjust with settings.req_topic
    #     self.client.get(f"/consume/{topic}")

    # Example: DataIn JSON
    @task
    def data_in(self):
        payload = {
            "streamID": str(random.randint(1, 10)),
            "dataSetkey": "Forex",
            "values": {"price": random.random() * 100}
        }
        self.client.post("/dataIn/", json=payload)

    def build_params(self, synopsis_id: int):
        if synopsis_id == 1:  # countMin
            return [
                "StockID",              # KeyField
                "price",                # ValueField
                "Queryable",            # OperationMode
                str(random.randint(1, 10)),   # epsilon
                str(random.randint(50, 100)), # cofidence
                str(random.randint(1, 1000))  # seed
            ]
        elif synopsis_id == 2:  # bloomFilter
            return [
                "StockID",
                "price",
                "Queryable",            # OperationMode
                str(random.randint(100, 1000)),  # numberOfElements
                str(random.randint(1, 10))       # FalsePositive
            ]
        elif synopsis_id == 3:  # ams
            return [
                "StockID",
                "price",
                "Queryable",            # OperationMode
                str(random.randint(1, 20)),   # Depth
                str(random.randint(50, 200))  # Buckets
            ]
        else:
            return []



    # Example: Add Synopsis
    @task
    def add_synopsis(self):
        r_stream_id = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
        r_dataSKey = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
        synopsis_id = random.randint(1, 3)
        payload = {
            "dataSetkey": r_dataSKey,
            "streamID": r_stream_id,
            "synopsisID": synopsis_id,   # adjust based on your schemas
            "noOfP": 4,
            "param": self.build_params(synopsis_id)
        }
        with self.client.post("/requests/add", json=payload, catch_response=True) as resp:
            if resp.status_code != 200:
                resp.failure(f"Error {resp.status_code}: {resp.text}")

    # Example: Estimation
    # @task
    # def estimation(self):
    #     payload = {
    #         "externalUID": str(random.randint(1000, 9999)),
    #         "uid": random.randint(1, 100),
    #         "streamID": "1",
    #         "synopsisID": "SYN1",   # adjust with your schema
    #         "dataSetkey": "Forex",
    #         "param": ["p1", "p2"],
    #         "noOfP": 2,
    #         "cache_max_age": 10
    #     }
    #     self.client.post("/estimations/", json=payload)

