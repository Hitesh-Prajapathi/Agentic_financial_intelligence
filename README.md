# SEC Insider Trading Agentic Pipeline 📊🤖

A production-grade, multi-agent AI system designed to analyze real-time SEC Form 4 insider trading filings, correlate them with social sentiment via X (Twitter), and expose a conversational chatbot interface capable of data retrieval and dynamic chart generation. This project was developed as a comprehensive solution for the CrowdWisdomTrading AI Agent assessment.

---

## 🎯 Project Scope & Deliverables Achieved

This project fulfills and exceeds all requirements outlined in the assessment:

1. **Top Insider Trading Filter**: Automatically fetches all SEC Form 4 insider trading data from the last 24 hours, parses out the net dollar flow, and identifies the Top 5 most significant trades by value.
2. **Social Sentiment Context (Apify)**: Dynamically triggers Apify's Twitter/X scraper (`apidojo/tweet-scraper`) to gather context on the correlated tickers for the last 7 days.
3. **Optimized Chat Bot (LightRAG)**: Uses **LightRAG** (a graph-based RAG architecture) to chunk, index, and retrieve structured context. The chatbot answers strictly based on this RAG context or its local database. context to prevent hallucinations.
4. **Dynamic Chart Generation**: Detects user intent ("draw me a chart of the top trades") and completely bypasses the standard LLM response path to dynamically generate and serve native data visualizations (Matplotlib).
5. **Closed Learning Loop**: Implements OpenTelemetry monitoring (`telemetry/otel_setup.py`) to trace agent logic, alongside a dedicated `learning_loop.py` skill that logs queries, measures confidence scores, and iteratively refines agent prompt contexts based on failure modalities.
6. **Scale, Logging, Error Handling (Extra Points)**: The architecture is built with dependency injection, strict schema validation (via Pydantic equivalents in Prompts), graceful fallback states (e.g., loading Demo Data to bypass 24hr SEC API windows), and robust, noise-filtered Python `logging`. 

---

## 🏛️ System Architecture

The pipeline consists of modular micro-agents coordinated by a `SupervisorAgent`:

- **RetrievalAgent**: Loads SEC filings and handles Graph RAG extraction.
- **SocialScraperAgent**: Interfaces with Apify to pull current social signals.
- **SentimentAgent**: Grades tweets using a fast LLM model (`mistralai/mixtral-8x7b-instruct`) for bullish/bearish lean.
- **VisualizationAgent**: Intercepts chart intents to generate live plot images.
- **ChatAgent**: Synthesizes the extracted entities and structured data into a final response. 

**Tech Stack**: Python 3.10, Streamlit (UI), DuckDB (Fast in-memory/disk DB), LightRAG, OpenRouter (LLM routing), OpenTelemetry.

---

## 🚀 Setup & Installation

**1. Clone the repository:**
```bash
git clone <your-repository-url>
cd Insider_trading_project
```

**2. Create a virtual environment & install requirements:**
```bash
conda create -n Insi_trade_venv python=3.10 -y
conda activate Insi_trade_venv
pip install -r requirements.txt
```

**3. Configure Environment Variables:**
Add the following tokens to a `.env` file in the root directory:
```env
OPENROUTER_API_KEY="your_openrouter_key"
APIFY_API_TOKEN="your_apify_token"
```

**4. Run the UI:**
```bash
PYTHONPATH=. streamlit run app.py
```
> **Tip:** If there are no Form 4 filings on the SEC in the last 24 hours, use the **"Load Demo Data"** button in the Streamlit sidebar to inject realistic mock data and test the full pipeline.

---

## 📸 Sample Input & Output

### 1. Conversational Queries & Sourced Answers
The ChatAgent relies strictly on extracted data to build contextually rich answers.

*Sample 1:*
![Input Output 1](./Screenshot%202026-04-14%20at%206.50.19%E2%80%AFPM.png)

*Sample 2:*
![Input Output 2](./Screenshot%202026-04-14%20at%206.50.45%E2%80%AFPM.png)

### 2. Dynamic Chart Generation
By detecting visualization intent, the agent routes the command to the Visualization tier instead of hallucinating textual charts.

*Sample 3:*
![Input Output 3](./Screenshot%202026-04-14%20at%206.50.54%E2%80%AFPM.png)

*Sample 4:*
![Input Output 4](./Screenshot%202026-04-14%20at%206.51.03%E2%80%AFPM.png)

![Input Output 5](./Screenshot%202026-04-14%20at%206.51.10%E2%80%AFPM.png)

![Input Output 6](./Screenshot%202026-04-14%20at%206.51.17%E2%80%AFPM.png)

---

## 🔑 APIFY Token Usage Tracking

As requested, the Apify Actor has been heavily utilized to test social retrieval workflows. The system automatically restricts the Actor to the Top 5 tickers to optimize CU spending.

It is shown in the recording of the whole project execution attached in the email

---

## 🧠 The "Closed Learning Loop"

This specific requirement was fulfilled via an integration of OpenTelemetry + SQLite validation rules:
1. **Telemetry**: Every operation (OpenRouter token sizes, prompt times, API latency) is tracked as a Span instance. 
2. **Confidence Measurement**: The Supervisor Agent grades output structures.
3. **Loop Activation**: We utilize the `skills/learning_loop.py` script. The Telemetry data provides "feedback" over which prompt models are hallucinating variables vs extracting perfectly, dynamically shifting the pipeline away from failure points based on historical traces.

---
**Thank you for your review!** Please feel free to test the repository locally, the codebase has been extensively reviewed to ensure seamless setup.
