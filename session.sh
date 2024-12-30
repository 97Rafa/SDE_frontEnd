#!/bin/bash

# Create a new tmux session named 'my_session'
tmux new-session -d -s my_session

# Rename the first window
tmux rename-window -t my_session:0 'Main'

# Split the window into panes
tmux split-window -v  # Split into two panes horizontally
tmux select-pane -t 1 # Go to the second (right) pane
tmux split-window -h  # Split the first pane vertically
tmux split-window -h  # Split the first pane vertically
tmux select-pane -t 0
tmux split-window -h  # Split the second pane vertically
tmux split-window -h  # Split the second pane vertically

# Pane 0
tmux send-keys -t my_session:0.0 'make quick' C-m

# Pane 1
tmux send-keys -t my_session:0.1 'cd ~/Workspace/SDE; sleep 5; flink run target/SDE-1.0.1-SNAPSHOT.jar' C-m

# Pane 2
tmux send-keys -t my_session:0.2 'source .kafka-venv/bin/activate; sleep 5; python polling_API/monitorV2.py' C-m

# Pane 3
tmux send-keys -t my_session:0.3 'sleep 12;docker exec -it kafka-container  /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic request_topic --from-beginning' C-m

# Pane 4
tmux send-keys -t my_session:0.4 'sleep 12;docker exec -it kafka-container  /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic data_topic --from-beginning' C-m

# Pane 5
tmux send-keys -t my_session:0.5 'sleep 12;docker exec -it kafka-container  /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic estimation_topic --from-beginning' C-m

# Attach to the session
tmux attach-session -t my_session
