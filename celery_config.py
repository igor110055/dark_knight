## Broker settings.
broker_url = 'amqp://guest:guest@localhost:5672//'
# broker_url = 'redis://localhost:6379/1'

# List of modules to import when the Celery worker starts.
imports = ('tasks.order_book_task', 'tasks.websocket_task', 'tasks.order_task')

## Using the database to store task state and results.
result_backend = 'redis://localhost:6379/2'

# task_annotations = {'tasks.add': {'rate_limit': '10/s'}}

# task_routes = {
#     'tasks.order_book_task.update_order_book': 'default',
#     'tasks.order_book_task.get_order_book_snapshot': 'additional'
# }

beat_schedule = {
    'ping-websocket-server-every-30-seconds': {
        'task': 'tasks.websocket_task.ping_server',
        'schedule': 1.0
    }
}

timezone = 'UTC'