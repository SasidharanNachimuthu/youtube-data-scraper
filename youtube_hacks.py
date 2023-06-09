import streamlit as st
import googleapiclient.discovery
import googleapiclient.errors
import json
import pymongo
import mysql.connector
import isodate
import pandas as pd



def youtube_api_connect(API_KEY):
    youtube = googleapiclient.discovery.build("youtube", "v3", developerKey=API_KEY)
    return youtube

def data(youtube,c_id):
    #st.write(c_id)
    
    try:
        all_data ={}
        channel_data = get_channel_data(youtube,c_id)
        playlist_data,nofplaylist = get_playlists_data(youtube,c_id)
        
        c_data=dict(
            Channel_name = channel_data['snippet']['title'],
            Channel_Id = channel_data['id'],
            Subscription_count = channel_data['statistics']['subscriberCount'],
            Channel_views = channel_data['statistics']['viewCount'],
            Channel_description = channel_data['snippet']['description'],
            Playlist_count = nofplaylist,
            Playlists = playlist_data
        )
        data = c_data.copy()
        
        all_data['channel']=c_data
        
        
    
        #file_name = 'channeldata' + '.json'
        #with open(file_name, 'w') as f:
        #    json.dump(all_data, f, indent=4)
        #   print('file dumped')
            
        if 'Playlists' in data:
            del data['Playlists']
        
        
        return data,all_data
    
    except googleapiclient.errors.HttpError as e:
        st.write("An HTTP error occurred:")
        st.write(e)
        
    
        
def get_channel_data(youtube,CHANNEL_ID):
    channel_request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=CHANNEL_ID
    )
    channel_response = channel_request.execute()
    
    
    if channel_response["items"]:
        channel_data = channel_response["items"][0]
        return channel_data
    return None    
        
        
def get_playlists_data(youtube,CHANNEL_ID):
    playlists_request = youtube.playlists().list(
                part="snippet",
                channelId=CHANNEL_ID,
                maxResults=50  
        )
            
    playlists_response = playlists_request.execute()
            
    playlists= []
    while playlists_request is not None:
        playlists_response = playlists_request.execute()
        playlists += playlists_response["items"]
        playlists_request = youtube.playlists().list_next(playlists_request, playlists_response)
        
    if playlists:
        playlists_data=[]
        nofplaylist = len(playlists)
        for i in range(nofplaylist):
            playlistitems_request = youtube.playlistItems().list(
                    part = "snippet",
                    playlistId = playlists[i]['id'],
                    maxResults = 50
            )
            playlistitems_response = playlistitems_request.execute()
            
            playlist_items = []
    
    
            while playlistitems_request is not None:
                playlistitems_response = playlistitems_request.execute()
                playlist_items += playlistitems_response["items"]
                playlistitems_request = youtube.playlistItems().list_next(playlistitems_request, playlistitems_response)
                            
            if playlist_items:
                nofplaylistitems = len(playlist_items)
            else:
                st.write("No playlists found.")
                nofplaylist = 0
            videos_data=get_video_data(youtube,playlist_items,nofplaylistitems)

                        
            playlist=dict(
                playlist_name = playlists[i]['snippet']['title'],
                playlist_id = playlists[i]['id'],
                videos = videos_data
                )
            playlists_data.append(playlist)
        
    return playlists_data,nofplaylist    
    
        
def get_video_data(youtube,playlist_items,nofplaylistitems):
    videos_data=[]
    #print(nofplaylistitems)
    for z in range(nofplaylistitems):
        video_request = youtube.videos().list(
            part = "snippet,contentDetails,statistics,status",
            id = playlist_items[z]['snippet']['resourceId']['videoId'],
        ) 
        video_response = video_request.execute()
        if len(video_response['items'])==0:
            #print("Private video")
            continue
        else:
            videos = video_response['items'][0]


        if 'tags' in videos['snippet']:
            tags = videos['snippet']['tags']
        else:
            tags = "Tags Not available"
        comments_data = get_comments_data(youtube,playlist_items[z]['snippet']['resourceId']['videoId'])
        
        dur = isodate.parse_duration(videos['contentDetails']['duration'])
        tdur = int(dur.total_seconds())
        
        v_dict = dict(
            Video_Id = videos['id'],
            Video_Name = videos['snippet']['title'],
            Video_Description = videos['snippet']['description'],
            Tags = tags,
            PublishedAt = videos['snippet']['publishedAt'],
            View_Count = videos['statistics']['viewCount'],
            Like_Count = videos['statistics']['likeCount'],
            Dislike_Count = 0, #videos['statistics']['dislikeCount'],
            Favourite_Count =0, #videos['statistics']['favouriteCount'],
            Comment_Count = videos['statistics']['commentCount'],
            Duration = tdur,
            Thumbnail= videos['snippet']['thumbnails']['default']['url'],
            Caption_Status = "Available" if videos['contentDetails']['caption'] else "Not Available",
            Comments =comments_data                 
            )
        videos_data.append(v_dict)
    return videos_data
    
def get_comments_data(youtube,playlistitemId):
    comments =[]    
    next_page_token = None
    while True:
        comments_request = youtube.commentThreads().list(
            part='snippet',
            videoId=playlistitemId,
            maxResults=100,
            pageToken=next_page_token
        )
        comments_response = comments_request.execute()

        comments +=comments_response['items']

        if 'nextPageToken' in comments_response:
            next_page_token = comments_response['nextPageToken']
        else:
            break
    c_data=[]
    for j in range(len(comments)):
        c_dict = dict(
            Comment_Id = comments[j]['id'],
            Comment_Text = comments[j]['snippet']['topLevelComment']['snippet']['textDisplay'],
            Comment_Author = comments[j]['snippet']['topLevelComment']['snippet']['authorDisplayName'],
            Comment_PublishedAt = comments[j]['snippet']['topLevelComment']['snippet']['publishedAt']
        )
        c_data.append(c_dict)
    return c_data

def store_data_mongo(alldata):
    # Set up MongoDB connection
    mongourl = st.secrets["MONGOURL"]
    try :
        client = pymongo.MongoClient(mongourl)
        db = client['YoutubeHacks']
        collection = db['ChannelData']
        #jsondata = open('channeldata.json')
        channel_data = alldata
        fields = "channel.Channel_Id"
        value = channel_data["channel"]["Channel_Id"]
        if collection.find_one({fields:value}):
            return value
        else:
            insert_result = collection.insert_one(channel_data)

        # Check if the data was successfully inserted
        if insert_result.inserted_id:
            client.close()
            return value
        else:
            st.write("Failed to insert data.")
            
        
        
    except Exception as e:
        st.write("An Error Occurred!")
        st.write(e)
        
def sql_connect():
    sql_host = st.secrets["DB_HOST"]
    sql_user = st.secrets["DB_USER"]
    sql_password = st.secrets["DB_PASS"]
    dbname = st.secrets["DB_NAME"]    
    sql_port = st.secrets["DB_PORT"]
    conn = mysql.connector.connect(
            host=sql_host,
            port=sql_port,
            user=sql_user,
            password=sql_password,
            database = dbname
        )
    return conn


def store_data_sql(filterdata):
    
    try:
        
        conn = sql_connect()
        
        if conn:
            cursor = conn.cursor()
            mongourl = st.secrets["MONGOURL"]
            mongoclient = pymongo.MongoClient(mongourl)
            db = mongoclient['YoutubeHacks']
            collection = db['ChannelData']
            fields = "channel.Channel_Id"
            document = collection.find_one({fields:filterdata})
            
            
            
            check_query = f"SELECT channel_id FROM channeldata WHERE channel_id = '{filterdata}'"
            
            cursor.execute(check_query)

            # Fetch the result
            result = cursor.fetchone()
            
            if result:
                st.write("Channel Data Inserted Already!. Try with another Channel")
            else:            
                # Insert Channel data
                q1="INSERT INTO channeldata (channel_id, channel_name, subscription_count, channel_views, playlist_count, channel_description) VALUES (%s, %s, %s, %s, %s, %s)"
                data = (document['channel']['Channel_Id'],document['channel']['Channel_name'],document['channel']['Subscription_count'],document['channel']['Channel_views'],document['channel']['Playlist_count'],document['channel']['Channel_description'])
                cursor.execute(q1,data)
                
                # Insert Playlist Data
                data2=[]
                for i in document['channel']['Playlists']:
                    st.write(i['playlist_name'])
                    data2.append((i['playlist_id'],document['channel']['Channel_Id'],i['playlist_name']))
                    # Insert video Data
                    data3=[]
                    for j in i['videos']:
                        data3.append((j['Video_Id'],i['playlist_id'],j['Video_Name'],j['Video_Description'],j['PublishedAt'],j['View_Count'],j['Like_Count'],j['Comment_Count'],j['Duration'],j['Thumbnail'],j['Caption_Status']))
                        # Insert Comment Data
                        data4 = []
                        for z in j['Comments']:
                            data4.append((z['Comment_Id'],j['Video_Id'],z['Comment_Text'],z['Comment_Author'],z['Comment_PublishedAt']))
                        q4 = "INSERT INTO commentdata(comment_id,video_id,comment_text,comment_author,comment_published_date) VALUES (%s,%s,%s,%s,%s)"    
                        cursor.executemany(q4,data4)
                    q3 = "INSERT INTO videodata(video_id,playlist_id,video_name,video_description,published_date,view_count,like_count,comment_count,duration,thumbnail,caption_status) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    cursor.executemany(q3,data3)
                q2 = "INSERT INTO playlistdata(playlist_id,channel_id,playlist_name) VALUES(%s,%s,%s)"
                cursor.executemany(q2,data2)
                st.write("Channel Data Stored!")
            conn.commit()

                
    except mysql.connector.Error as err:
        st.write("Error: {}".format(err))       
        
         
#if err.errno == mysql.connector.errorcode.ER_BAD_DB_ERROR:
#            c = create_database(dbname, sql_host, sql_user, sql_password)
#            if c: 
#                st.write()
#                store_data_sql(filterdata)
#        else:
#            st.write("Error: {}".format(err))"""
            
            
            
def create_database(db,sql_host,sql_user,sql_pass):
    try:
        conn = mysql.connector.connect(
            host=sql_host,
            user=sql_user,
            password=sql_pass,
        )
        cursor = conn.cursor()
        cursor.execute("CREATE DATABASE {}".format(db))
        conn.close()
        conn = sql_connect()
        if conn:  
            st.write("Database created")
            cursor = conn.cursor()
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()    
            dtable = ['channeldata','commentdata','playlistdata','videodata']
            for i,table in zip(dtable,tables):
                if i.lower() != table[0].lower():
                    if i=='channeldata':
                        cursor.execute("create table channeldata(channel_id varchar(255),channel_name varchar(255),subscription_count int,channel_views int,playlist_count int,channel_description text)")
                    elif i=='commentdata':
                        cursor.execute("create table commentdata(comment_id varchar(255),video_id varchar(255),comment_text text,comment_author varchar(255),comment_published_date datetime)")
                    elif i=='playlistdata':
                        cursor.execute("create table playlistdata(playlist_id varchar(255),channel_id varchar(255),playlist_name varchar(255))")
                    elif i=='videodata':
                        cursor.execute("create table videodata(video_id varchar(255),playlist_id varchar(255),video_name varchar(255),video_description text,published_date datetime,view_count int, like_count int,comment_count int,duration int,thumbnail varchar(255),caption_status varchar(255))")
            conn.close()
        return True
            
    except mysql.connector.Error as err:
        st.write("Error: {}".format(err))
        return False
    
def query_sql_data(pos):
    conn = sql_connect()
    cursor = conn.cursor()
    if pos==1:
        q = "SELECT vd.video_name, cd.channel_name FROM videodata AS vd JOIN playlistdata AS pd ON vd.playlist_id = pd.playlist_id JOIN channeldata AS cd ON pd.channel_id = cd.channel_id"
    elif pos==2:
        q="SELECT cd.channel_name, COUNT(vd.video_id) AS video_count FROM channeldata AS cd JOIN playlistdata AS pd ON cd.channel_id = pd.channel_id JOIN videodata AS vd ON pd.playlist_id = vd.playlist_id GROUP BY cd.channel_name ORDER BY video_count DESC;"
    elif pos==3:
        q="SELECT vd.video_name, cd.channel_name, vd.view_count FROM videodata AS vd JOIN playlistdata AS pd ON vd.playlist_id = pd.playlist_id JOIN channeldata AS cd ON pd.channel_id = cd.channel_id ORDER BY vd.view_count DESC LIMIT 10;"
    elif pos==4:
        q="SELECT vd.video_name, COUNT(c.comment_id) AS comment_count FROM videodata AS vd LEFT JOIN commentdata AS c ON vd.video_id = c.video_id GROUP BY vd.video_name;"
    elif pos==5:
        q="SELECT vd.video_name, cd.channel_name, vd.like_count FROM videodata AS vd JOIN playlistdata AS pd ON vd.playlist_id = pd.playlist_id JOIN channeldata AS cd ON pd.channel_id = cd.channel_id WHERE (vd.like_count, cd.channel_id) IN ( SELECT MAX(v.like_count), p.channel_id FROM videodata AS v JOIN playlistdata AS p ON v.playlist_id = p.playlist_id GROUP BY p.channel_id ) ORDER BY vd.like_count DESC;"
    elif pos==6:
        q="SELECT vd.video_name, cd.channel_name, vd.like_count FROM videodata AS vd JOIN playlistdata AS pd ON vd.playlist_id = pd.playlist_id JOIN channeldata AS cd ON pd.channel_id = cd.channel_id ORDER BY vd.like_count DESC;"
    elif pos==7:
        q="SELECT cd.channel_name, SUM(vd.view_count) AS total_views FROM channeldata AS cd JOIN playlistdata AS pd ON cd.channel_id = pd.channel_id JOIN videodata AS vd ON pd.playlist_id = vd.playlist_id GROUP BY cd.channel_name;"
    elif pos==8:
        q="SELECT DISTINCT cd.channel_name FROM channeldata AS cd JOIN playlistdata AS pd ON cd.channel_id = pd.channel_id JOIN videodata AS vd ON pd.playlist_id = vd.playlist_id WHERE YEAR(STR_TO_DATE(vd.published_date, '%Y-%m-%dT%H:%i:%sZ')) = 2022;"
    elif pos==9:
        q="SELECT cd.channel_name, AVG(vd.duration) AS average_duration FROM channeldata AS cd JOIN playlistdata AS pd ON cd.channel_id = pd.channel_id JOIN videodata AS vd ON pd.playlist_id = vd.playlist_id GROUP BY cd.channel_name;"
    elif pos==10:
        q="SELECT vd.video_name, cd.channel_name, COUNT(cm.comment_id) AS comment_count FROM videodata AS vd JOIN playlistdata AS pd ON vd.playlist_id = pd.playlist_id JOIN channeldata AS cd ON pd.channel_id = cd.channel_id JOIN commentdata AS cm ON vd.video_id = cm.video_id GROUP BY vd.video_name, cd.channel_name ORDER BY comment_count DESC;"
    if pos:
        cursor.execute(q)
        rows = cursor.fetchall()
        df = pd.DataFrame(rows, columns=cursor.column_names)
        df.index = df.index+1
        return df
    


def main():
    st.header("Youtube Hacks")
    
    tab1, tab2, tab3 = st.tabs(["Data", "Table","Query"])

    with tab1:
        st.header("Fetch Data")
        apikey = st.text_input("API Key:")
        c_id = st.text_input("Channel ID:")
        if c_id and apikey:
            youtube = youtube_api_connect(apikey)
            chdata,alldata = data(youtube,c_id)
            st.json(chdata)
            
            if st.button("Store Data") :
                filterdata = store_data_mongo(alldata)
                store_data_sql(filterdata)
                
                
    with tab2:
        st.header("Table")
        conn = sql_connect()
        if conn:
            cursor = conn.cursor()
            q1  = "SELECT * FROM channeldata"
            cursor.execute(q1)
            rows = cursor.fetchall()

            # Convert the rows to a DataFrame
            df = pd.DataFrame(rows, columns=cursor.column_names)
            df.index = df.index + 1

            # Display the DataFrame as a table in Streamlit
            st.write("Channels List")
            
            table_style = f"""
                <style>
                    .dataframe tbody tr {{
                        height: 30px;
                    }}
                    .dataframe table {{
                        height: 200px;
                        width: 1000px;
                    }}
                    .dataframe thead th:first-child,
                    .dataframe tbody tr:first-child {{
                        background-color: #f2f2f2;
                        position: sticky;
                        top: 0;
                        z-index: 1;
                    }}
                </style>
            """
            st.markdown(table_style, unsafe_allow_html=True)
            st.dataframe(df)
            
            
            q2  = "SELECT * FROM playlistdata"
            cursor.execute(q2)
            playlist_rows = cursor.fetchall()
            playlist_df = pd.DataFrame(playlist_rows, columns=cursor.column_names)
            playlist_df.index = playlist_df.index + 1
            st.write("Playlist List")
            st.dataframe(playlist_df)
            
            q3  = "SELECT * FROM videodata"
            cursor.execute(q3)
            video_rows = cursor.fetchall()
            video_df = pd.DataFrame(video_rows, columns=cursor.column_names)
            video_df.index = video_df.index + 1
            st.write("Video List")
            st.dataframe(video_df)

            q4  = "SELECT * FROM commentdata"
            cursor.execute(q4)
            comment_rows = cursor.fetchall()
            comment_df = pd.DataFrame(comment_rows, columns=cursor.column_names)
            comment_df.index = comment_df.index + 1
            st.write("Comment List")
            st.dataframe(comment_df)
        
    
    with tab3:
        st.header("Query")
        
        options = ["Select a Question from the List","What are the names of all the videos and their corresponding channels?","Which channels have the most number of videos, and how many videos do they have?",
                   "What are the top 10 most viewed videos and their respective channels?","How many comments were made on each video, and what are their corresponding video names?",
                   "Which videos have the highest number of likes, and what are their corresponding channel names?","What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                   "What is the total number of views for each channel, and what are their corresponding channel names?","What are the names of all the channels that have published videos in the year 2022?",
                   "What is the average duration of all videos in each channel, and what are their corresponding channel names?","Which videos have the highest number of comments, and what are their corresponding channel names?"
                ]
        selected_option = st.selectbox("Ask a Question:", options)

        # Get the position of the selected option
        position = options.index(selected_option)
        
        retriveddata = query_sql_data(position)
        
        st.table(retriveddata)
        
if __name__ == "__main__":
    main()
