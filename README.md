🚀 Crypto Streamlit App

A real-time cryptocurrency analytics dashboard built with Streamlit, powered by Kafka streaming and Redis caching, and containerized using Docker.

📌 Features

📊 Live cryptocurrency data visualization (line charts, trends)

⚡ Real-time data ingestion via Kafka 

🗄️ Caching & quick lookups using Redis

🐳 Portable & reproducible setup with Docker

🌐 Exposes a Streamlit dashboard on http://localhost:8501
    with LLM support for giving insights 

🛠️ Tech Stack

Backend: Python 3.10

Frontend/Dashboard: Streamlit

Streaming: Apache Kafka

Cache/Store: Redis

Containerization: Docker

📂 Project Structure
crypto-streamlit-app/
│── Dockerfile
│── requirements.txt
│── streamlit.py          # main Streamlit app
│── kafka_consumer.py     # consumes live crypto trade data
│── redis_config.py       # Redis helper functions
│── utils/                # helper modules
└── README.md

⚙️ Installation & Setup
1️⃣ Clone the repository
git clone https://github.com/your-username/crypto-streamlit-app.git
cd crypto-streamlit-app

2️⃣ Build the Docker image
docker build -t crypto-streamlit_app:latest .

3️⃣ Run the container
docker run -d -p 8501:8501 --name crypto-streamlit crypto-streamlit_app:latest

4️⃣ Access the dashboard

Open your browser and go to 👉 http://localhost:8501

📊 Usage

The app fetches live crypto trades via Kafka.

Messages are cached in Redis for quick retrieval.

Streamlit displays an interactive dashboard with live charts.

🧑‍💻 Development (hot-reload)

To run locally without Docker:

pip install -r requirements.txt
streamlit run streamlit.py

🚀 Future Improvements

Add authentication for dashboard access

Support multiple crypto exchanges

Deploy to AWS/GCP with Kubernetes

Historical data analysis with PostgreSQL
