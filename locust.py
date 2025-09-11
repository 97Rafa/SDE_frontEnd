from locust import HttpUser, TaskSet, task, between

class UserBehavior(TaskSet):
    @task
    def get_items(self):
        self.client.get("/dataIn/")

    # @task
    # def create_item(self):
    #     self.client.post("/api/items", json={"name": "test", "value": 123})

class WebsiteUser(HttpUser):
    host = "http://localhost:4000"
    tasks = [UserBehavior]
    wait_time = between(1, 5)