# capstone1
Jeriyl-capstone1 Project

# To Install the required packages
pip install requirements.txt

show_table=st.radio("Select the button for view",("Channel","Video","Comments","Playlist"))
        if show_table=="Channel":
            show_channel_table()
        elif show_table=="Video":
            show_video_table()
        elif show_table=="Comments":
            show_comments_table()
        elif show_table=="Playlist":
            show_playlist_table()
        else:
            print("Choose the appropriate option")

