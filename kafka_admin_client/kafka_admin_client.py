from kafka.admin import KafkaAdminClient, NewTopic, ConfigResource, ConfigResourceType

admin_client = KafkaAdminClient(
    bootstrap_servers=['localhost:9092'],
    client_id='dataops_client'
)
topic_list = admin_client.list_topics()
def create_new_topic():
    try:
        admin_client.create_topics(new_topics=[NewTopic('office_input', 1, 1)])
        return "Topic office_input successfully created"
    except:
        return "Topic office_input already exists"

if __name__ == "__main__":
    create_new_topic()