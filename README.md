# YouTube Data Scraper

A YouTube Data Scraper built with **Python**, using the **YouTube Data API**, **MongoDB**, **MySQL**, and **Streamlit**. This application fetches YouTube channel, playlist, video, and comment data, stores it in MongoDB for raw storage, transfers structured data into MySQL, and provides an interface for analysis.

---

## Features
- **Fetch YouTube Data:** Retrieve channel, playlist, video, and comment data using the YouTube API.
- **Store in MongoDB:** Raw data is stored in MongoDB for easy retrieval.
- **Migrate to MySQL:** Data is transferred to MySQL for structured analysis.
- **Query Functionality:** Perform SQL queries to analyze YouTube data.
- **User-Friendly Interface:** Built with Streamlit for easy interaction.

---

## Technologies Used
- **Python** (Backend processing)
- **YouTube Data API v3** (Fetching YouTube data)
- **MongoDB** (Storing raw data)
- **MySQL** (Structured data storage & querying)
- **Streamlit** (Web-based UI for user interaction)

---

## Installation
### Prerequisites
Ensure you have the following installed:
- Python 3.x
- MongoDB
- MySQL Server
- Google Cloud API Key (for YouTube API access)

### Clone the Repository
```bash
git clone https://github.com/SasidharanNachimuthu/youtube-data-scraper.git
cd youtube-data-scraper
```

### Install Dependencies
```bash
pip install -r requirements.txt
```

### Set Up API Key
1. Get a **YouTube Data API Key** from Google Cloud.
2. Add your API key to an `.env` file in the project root:
```env
YOUTUBE_API_KEY=your_api_key_here
```

### Run MongoDB & MySQL
Start your **MongoDB** and **MySQL** services before running the script.

### Run the Application
```bash
streamlit run app.py
```

---

## Usage
1. **Enter a YouTube Channel ID** in the Streamlit app.
2. **Fetch and store data** in MongoDB.
3. **Transfer data to MySQL** for structured storage.
4. **Run SQL queries** to analyze the data.

---

## Database Structure
### MongoDB Collections
- `channels`
- `playlists`
- `videos`
- `comments`

### MySQL Tables
- `Channels`
- `Playlists`
- `Videos`
- `Comments`

---

## Future Enhancements
- Optimize YouTube API calls to reduce rate limits.
- Implement incremental data updates.
- Add graphical data visualization.

---

## Contributing
Contributions are welcome! Feel free to fork this repository and submit a pull request.

---

## Author
**Sasidharan Nachimuthu**  
[GitHub Profile](https://github.com/SasidharanNachimuthu)

