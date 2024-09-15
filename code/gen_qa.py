'''
'''


import json
import os
import random
import time
from multiprocessing import Manager, Pool
from pathlib import Path

import pandas as pd
from langchain_openai import AzureChatOpenAI, ChatOpenAI

general_questions_answers = [
    {
        "question": "What is the tempo/beat of this song?",
        "answer": "The beat starts at 0.11 second, BPM is 84.02, and it ends at 27.56 seconds. This is a song in 3/4 time signature."
    },
    {
        "question": "Can you present the structure of this song?",
        "answer": "The song structure is silence-intro-verse-instrumental-verse-instrumental-silence."
    },
    {
        "question": "Can you analyze the chord progression of this song?",
        "answer": "The chords in this song are F-G-Em-Am-Dm-G-C."
    },
    {
        "question": "What is the key of this song?",
        "answer": "The song is in F major."
    },
    {
        "question": "What genre is this song?",
        "answer": "Pop."
    },
    {
        "question": "What mood does this song represent?",
        "answer": "It represents a funny and loving mood."
    },
    {
        "question": "What language is this song in?",
        "answer": "It is in English and Chinese."
    },
    {
        "question": "What instruments are included in this song?",
        "answer": "Violin."
    },
    {
        "question": "What type of vocals does this song have?",
        "answer": "The song is performed by a male vocalist."
    },
    {
        "question": "What genre could this song be, based on its instrumentation?",
        "answer": "The song includes vocals, electric guitar, keyboard, bass, and drums. It could be rock, punk, or pop."
    },
    {
        "question": "Describe the dynamic range of this song.",
        "answer": "The song starts softly in the intro, gradually increases in loudness towards the chorus, and then softens again in the outro."
    },
    {
        "question": "Can you analyze this song's chord progression? Does the song modulate to a different key?",
        "answer": "The chord progression is F-G-Em-Am-Dm-G-C, common in pop song choruses. The song modulates from F major to A major."
    },
    {
        "question": "What is the time signature of this song?",
        "answer": "This is a song in 3/4 time, with a rhythm characteristic where the first beat is strong, and the following two are weaker."
    },
    {
        "question": "Is this song fast?",
        "answer": "The song's BPM is 50, making it a very slow song."
    },
    {
        "question": "Can you describe this song from an audio perspective?",
        "answer": "This song has a BPM of 50, making it very slow. It is in 3/4 time, giving it a dance rhythm feel."
    },
    {
        "question": "Can you recommend a song for me? How does this song sound? Can you recommend another song similar to this one?",
        "answer": "Sure, I recommend 'xxx.' This song has a beat and tempo that categorize it as this particular genre."
    },
    {
        "question": "Detail the style of this music.",
        "answer": "With a tempo of 150, a 4/4 time signature, primarily synthesizer sounds, and a 6415 chord progression in an ABAB structure, it is classified as electronic dance music."
    },
    {
        "question": "What emotions are conveyed through the instrumentation of this song?",
        "answer": "The gentle use of piano and strings conveys a sense of melancholy and introspection."
    },
    {
        "question": "How does this song use music to tell a story?",
        "answer": "Through its progressive structure, from a gentle beginning to a climactic end, and its lyrical content, the song narrates a journey of personal growth and overcoming challenges."
    },
    {
        "question": "Can you pinpoint the moments when the violin emerges prominently in the track, and describe its interaction with other instruments?",
        "answer": "Look for the violin's entry in the second verse, where it weaves a melancholic melody above the steady pulse of the piano, creating a poignant counterpoint that enhances the song's emotional depth."
    },
    {
        "question": "By analyzing the unique blend of electronic synths and classical piano in this track, can you deduce the artist or the title that characterizes this innovative fusion?",
        "answer": "Given the distinctive fusion of electronic and classical elements, this track could likely be the work of an artist known for genre-blending compositions, such as Ólafur Arnalds. The title might reflect the juxtaposition of modernity and tradition, such as Neo Classical Pulse."
    },
    {
        "question": "Find a similar song with the same bpm",
        "answer": "This song's bpm is <bpm_info>, and is a <genre_info> music, so I recommend this song: <song_name>."
    },
    {
        "question": "Find a similar song with the same mood",
        "answer": "Sure, this is a <mood_info> <genre_info> song, so I recommend this song: <song_name>."
    },
    {
        "question": "Give me some music with the same artist",
        "answer": "This song's artist is <artist_info>, and is in album <album_info>, so I recommend this song list: <song_list>."
    },
    {
        "question": "What's the style of this song? Any similar songs?",
        "answer": "This song is a <language_info> <mood_info> <genre_info>, so I recommend this song list: <song_list>."
    },
    {
        "question": "I want to use piano to harmonize this song, what's the chords for this song?",
        "answer": "This song is in key <key_info>, so you can play 4-5-3-6-2-5-1-1 as an eight bar chord progression, and the chords are: <chord_list>."
    },
    {
        "question": "I want to use my guitar to harmonize this song, what's the chords for this song?",
        "answer": "This song is in key <key_info>, so you can play 4-5-3-6-2-5-1-1 as an eight bar chord progression, and the chords are: <chord_list>. You can place the capo on the 2nd fret. After placing the capo on the 2nd fret, now you should play the new chords: [New_Chord_List]."
    },
    {
        "question": "The key of this song is too high, I want to play it in a lower key, maybe transpose the song down by two semitones.",
        "answer": "This song is in key <key_info>, and the chord progression are: <chord_list>. After transposing the song down by two semitones, the new key is <new_key_info>, now the chord progression are: [New_Chord_List]."
    }
]

function_calling_questions_answers = [
    {
        "question": "Can you find Jazz music with medium tempo for me?",
        "answer": "music_search(genre='jazz', tempo='medium')"
    },
    {
        "question": "How do I find calming music without vocals?",
        "answer": "music_search(mood='calm', vocals='none')"
    },
    {
        "question": "Can you locate high-energy electronic tracks for my workout?",
        "answer": "music_search(genre='electronic', energy='high', activity='workout')"
    },
    {
        "question": "I'm looking for music for a beach party, any suggestions?",
        "answer": "music_recommend(keywords='beach party')"
    },
    {
        "question": "What music do you suggest for focus while studying?",
        "answer": "music_recommend(keywords='study focus')"
    },
    {
        "question": "Can you recommend something for a cozy evening?",
        "answer": "music_recommend(keywords='cozy evening')"
    },
    {
        "question": "Can you find songs similar to this one?",
        "answer": "similar_music(audio_file='path/to/file')"
    },
    {
        "question": "I loved this track in a movie, can you find similar ones?",
        "answer": "similar_music(audio_file='path/to/movie/track')"
    },
    {
        "question": "This song's vibe is perfect for my mood, any more like it?",
        "answer": "similar_music(audio_file='path/to/song')"
    },
    {
        "question": "How can I trim the first 30 seconds from this song?",
        "answer": "music_edit(track_id='song123', action='trim', start_time=0, end_time=30)"
    },
    {
        "question": "How do I change the key of this track up by two semitones?",
        "answer": "music_edit(track_id='track456', action='key_change', semitones=2)"
    },
    {
        "question": "Can I increase the tempo of a track for a remix?",
        "answer": "music_edit(track_id='remix789', action='tempo_change', new_tempo=140)"
    },
    {
        "question": "Can you generate some relaxing music for meditation?",
        "answer": "music_gen(keywords='relaxing meditation music')"
    },
    {
        "question": "I need a song for a high-energy fitness video.",
        "answer": "music_gen(keywords='high-energy fitness')"
    },
    {
        "question": "Can we get a track that's perfect for a rainy day?",
        "answer": "music_gen(keywords='rainy day')"
    }
]



def init_globals():
    global question_count
    question_count = manager.dict()  # Use manager.dict() for a process-safe shared dictionary


def retrieve_random_qa():
    # Assuming the lists 'general_questions_answers' and 'function_calling_questions_answers' are defined outside this function
    selected_general_qa = random.sample(general_questions_answers, 6)
    selected_function_calling_qa = random.sample(function_calling_questions_answers, 2)

    # import ipdb; ipdb.set_trace()

    general_str = str(selected_general_qa)
    function_call_str = str(selected_function_calling_qa)


    # Simulating top questions tracking. In a real implementation, you would fetch this from a shared/global state.
    top_questions = []  # Placeholder for the top 5 most asked questions
    
    # Formatting the selected question-answer pair as strings
    formatted_general_str = general_str
    formatted_function_call_str = function_call_str
    
    # Retrieve and sort the top 50 questions based on count
    global question_count
    # Sort the items of the dictionary based on their count (value) in descending order
    sorted_questions = sorted(question_count.items(), key=lambda x: x[1], reverse=True)[:50]
    print(sorted_questions)
    
    # Format the sorted questions for display or further processing
    top_questions_str = "\n".join([f"Q: {q[0]}, Count: {q[1]}" for q in sorted_questions])
    
    return formatted_general_str, formatted_function_call_str, top_questions_str


# Process the original csv: https://chat.openai.com/share/0a01c338-5f84-4288-9613-115b7100d70e
# Generate questions
def format_prompt(beat_info, chord_info, key_info, structure_info, tagging_info, instrument_info):
    general_question_str, function_call_question_str, duplicated_questions_str = retrieve_random_qa()
    prompt= \
f"""You are an expert AI assistant that is knowledgeable about music production, musical structure, music history, and music styles, and you are hearing audio of a short clip of music. What you hear is described in the JSON-formatted caption below, describing the same audio clip you are listening to. Answer all questions as if you are hearing the audio clip. I will give you the following content:
- Music data format: introduce the format of Music meta
- Knowledge: the music knowledge you might use to describe music
- Tasks: your tasks
- Music meta: music info about one music

# Music data format:
- Genre
- Beat
    'time': the time stamp that has a "strong" beat detected from the music
    'beat': 1,2,3,... represents the beat from a music
- Chord: start|end|chord represents the start, end time of the chord
- Keymode
    'song': means the tune of the entire song
    'time|keymode': means the tune of this fragment


# Tasks
- Design a conversation between you and a person asking about this music. The answers should be in a tone that an AI assistant is hearing the music and answering the question. Ask diverse questions and give corresponding answers.
Again, do not ask about uncertain details. Provide detailed answers to all questions. For example, give detailed examples or reasoning steps to make the content more convincing and well-organized. Explain any musical concepts that would be unfamiliar to a non-musician. You can include multiple paragraphs if necessary. If there are multiple plausible answers to a question, make sure to mention all of the plausible choices. Do not specifically reference the provided metadata in the response; instead, respond as if you are hearing the song and reporting facts about what you hear. Do not mention the name of the artist or album in any of the questions. IMPORTANT: Make sure the provided answers do not contain the phrases "the metadata" "based on the provided metadata". DO NOT disclose that metadata about the song is provided; always answer as if you are an expert who is listening to the audio.
Make sure that the questions are complex, and that the detailed answers reflect your expertise as an expert AI assistant that is knowledgeable about music production, musical structure, music history, and music styles listening to the clip.
- You should ask question creatively, diversely.
- Stritly Return a single JSON list object containing the question-answer pairs. 
{{
        "qa": [{{"question": "", "answer": ""}}, ]
        "caption": ""
        "beat":""
        "chord":""
        "key":""
        "structure":
        "tagging":
        "instrument":
}}

Where 
qa is the conversation, you should genreate ~3 genral question and ~1 functino calling qa
caption is the comprehensive description of the music
beat, chord and key are short descriptions of the music from different perspective. Try to summarize what happend in the music, and list common pattern you find. These captions should be informative.

Eg: 
beat:  there are xx beats in this music, a typical beat sequence is xxxxx, 
caption: the music is xxx, where yyy, the chord is xxx, zzz
chord: This music uses a typical xx chord, where the sequence is a,b,c,d, in the begining of the music yy, the middle aa, then at end bb
key: The tune of the song is A, most of the tune is A, but in the middle of the music it's B, and ....
structure:
tagging:
instrument:



Again, you should only say something you can find from the meta data below, don't helucinate

# Knowledege
## You might ask questions from (not limited) from the following perspectives:
- Basic question
- Resoning question

## Function calling, you might as questions that stimulate user hope you to do some music stuff, and you should use the following tools:
Please also include "qustion" and "answer" key when gen function calling samples. Please put the function in "answer" key.
Eg:
{{
question: "Can you search a slow jazz music for me?"
answer: "music_search(genre='jazz', mood='introspective', tempo='slow')"
}}

Here are the function APIs:
music_search()
Searches for music based on detailed criteria such as genre, mood, tempo, and more.
Parameters:
genre: String, optional. The genre of the music.
mood: String, optional. The mood intended for the music.
tempo: String, optional. The tempo of the music, e.g., "slow", "medium", "fast".
energy: String, optional. The energy level of the music, e.g., "low", "medium", "high".
activity: String, optional. The activity or setting the music is intended for.
Example Call:
pythonCopy code
music_search(genre="jazz", mood="calm", tempo="medium")
music_recommend()
Recommends music based on a single "keywords" parameter, interpreting user intent for a variety of contexts.
Parameters:
keywords: String. Descriptive keywords that encapsulate the user's request for recommendations.
Example Call:
pythonCopy code
music_recommend(keywords="cozy evening")
similar_music()
Finds songs similar to an uploaded audio file, simplifying the discovery of music with similar vibes or characteristics.
Parameters:
audio_file: String. Path to the audio file for which similar music is sought.
Example Call:
pythonCopy code
similar_music(audio_file="path/to/file")
music_edit()
Provides various editing capabilities for music tracks, such as trimming, changing the key, and adjusting the tempo.
Parameters:
track_id: String. The identifier for the track to be edited.
action: String. The type of editing action to perform ("trim", "key_change", "tempo_change").
Additional parameters may be required based on the action, such as start_time, end_time, semitones, or new_tempo.
Example Call:
pythonCopy code
music_edit(track_id="song123", action="trim", start_time=0, end_time=30)
music_gen()
Generates new music based on provided "keywords" that describe the desired mood, style, or context.
Parameters:
keywords: String. Descriptive keywords that guide the generation of new music.
Example Call:
pythonCopy code
music_gen(keywords="high-energy fitness")

## Some typical questions and answers(the format is not correct, use the json format I tell you in the before):
### General question
{general_question_str}

### Functino calling questions
{function_call_question_str}

## Duplicated questions
Here are some questions that are dupicated, try to avoid generating them again!, you should be creative and ask questions that is unique。
{duplicated_questions_str}

# The music JSON content is:
## Beat
{beat_info}
## Chord
{chord_info}
## Key
{key_info}
## Structure
{structure_info}
## Tagging
{tagging_info}
# Instrument
{instrument_info}

# Your response
"""
    return prompt


# Simulate langchain's OpenAI API call function
# This function is a placeholder and does not execute in this environment
def call_openai_api(beat_info, chord_info, key_info, structure_info, tagging_info, instrument_info):
    # Simulate an API response
    s_t = time.time()
    prompt_str = format_prompt(beat_info, chord_info, key_info, structure_info, tagging_info, instrument_info)
    # res = llm.invoke(prompt_str).content

    model_name="gpt-4-0125-preview"
    # model_name="gpt-35-turbo-1106"
    # model_name="gpt-4-turbo-preview"
    llm = ChatOpenAI(
        model=model_name,
        temperature=0.95,
    ).bind(response_format={"type": "json_object"})
    res = llm.invoke(prompt_str).content
    print("Time cost for openai api:", time.time() - s_t)
    return res

# Reusing the base path and load_json function from before
def load_json(path):
    with open(path, 'r') as file:
        return json.load(file)
        
def process_music_json(music_id, base_path=None):
    if base_path == None:
        base_path = META_ROOT

    # TODO
    '''
    Will release the example json data later.
    '''
    META_BASE = "/mnt/bn/mir_data/"
    paths = {
        "beat": f"{META_BASE}/beat/{music_id}_beat.json",
        "beat_with_signature": f"{META_BASE}/beat_with_signature/{music_id}_beat.json",
        "key": f"{META_BASE}/key/{music_id}_key.json",
        "chord": f"{META_BASE}/chord/{music_id}_chord.json",
        "structure": f"{META_BASE}/structure/{music_id}_structure.json",
        "tagging": f"{META_BASE}/tagging/{music_id}_tagging.json",
        "instrument": f"{META_BASE}/instrument/{music_id}_instrument.json"
    }

    # Load and store each type of metadata
    metadata = {k: load_json(v) for k, v in paths.items()}

    # Example of how to process and use loaded metadata
    # This part should be customized based on how you intend to use the data
    beat_data = metadata['beat']
    chord_data = metadata['chord']
    key_data = metadata['key']
    structure_data = metadata['structure']
    tagging_data = metadata['tagging']
    instrument_data = metadata['instrument']


    # Process Beat Data correctly by including 'time' and 'value' arrays as per example
    beat_str = json.dumps(beat_data)  # Keep the beat data as is, similar to key and section

    # Chord Data Transformation
    chord_str = " | ".join([f"{seg['start']}|{seg['end']}|{seg['tag']}" for seg in chord_data['segment']])

    # Key and Section Data - Keep them the same
    key_str = json.dumps(key_data)  # Convert the entire JSON object to a string
    structure_str = json.dumps(structure_data)  # Convert the entire JSON object to a string
    tagging_str = json.dumps(tagging_data)
    instrument_str = json.dumps(instrument_data)

    return beat_str, chord_str, key_str, structure_str, tagging_str, instrument_str, beat_data, chord_data, key_data, structure_data, tagging_data, instrument_data



def process_file(row):
    # Function to process each file
    print("Process", row['id'])
    music_id = row['id']
    id_json_path = gpt_raw / f"{row['id']}.json"
    
    try:
        if not id_json_path.exists():
            # Prepare the MIR information
            beat_str, chord_str, key_str, structure_str, tagging_str, instrument_str, beat_data, chord_data, key_data, structure_data, tagging_data, instrument_data\
                = process_music_json(music_id)
            
            response = call_openai_api(beat_str, chord_str, key_str, structure_str, tagging_str, instrument_str)
            mir_info = {
                "beat": beat_data,
                "chord": chord_data,
                "key": key_data,
                "section": structure_data,  # Assuming section_data meant structure_data
                "tagging": tagging_data,
                "instrument": instrument_data
            }
            
            # Check if the response is a string and remove Markdown syntax if present
            if isinstance(response, str):
                response = json.loads(response.replace("```json", "").replace("```", ""))
            
            # Append MIR information to the response
            response['mir'] = mir_info
            
            with id_json_path.open('w') as f:
                json.dump(response, f)
        else:
            with id_json_path.open('r') as f:
                content_str = f.read().strip()
                # Remove Markdown code block syntax to parse as JSON
                content_str = content_str.replace("```json", "").replace("```", "")
                response = json.loads(content_str)
                assert len(content_str) > 10
            print("Jump finished json", row['id'])

        question_json = response
        # import ipdb; ipdb.set_trace()
        for qa_obj in question_json['qa']:
            question_str = qa_obj['question']
            with question_count_lock:  # Use a lock if needed for thread safety (optional here because manager.dict() is already process-safe)
                if question_str in question_count:
                    question_count[question_str] += 1
                else:
                    question_count[question_str] = 1

    except Exception as e:
        print(e)
        print("Fail", row['id'])


def build_final_csv():
    data = []
    for json_file in gpt_raw.glob('*.json'):
        with json_file.open() as f:
            content = json.load(f)
        id = json_file.stem
        # Format the JSON content with indentation and spaces
        gpt_caption = json.dumps(content, indent=4)
        data.append({'id': id, 'gpt_caption': gpt_caption})
    
    final_df = pd.DataFrame(data)
    # Save to CSV with proper quoting and escaping
    final_df.to_csv(gpt_csv)  # quoting=1 is QUOTE_ALL




if __name__ == "__main__":
    manager = Manager()
    question_count = manager.dict()  # Shared dictionary for question counts
    question_count_lock = manager.Lock()  # Optional: for explicit locking (manager.dict() operations are already atomic and process-safe)
    init_globals()  # Initialize global variables with shared structures

    # Initialize paths
    ROOT = Path('/mnt/bn//qa/')
    MIR_CSV = ROOT / 'indexes' / 'all.csv'
    CACHE = ROOT / 'cache'
    gpt_raw = CACHE / 'gpt4_caption_3_27_5'
    gpt_csv = CACHE / 'gpt4_caption_3_27.csv'
    META_ROOT = ROOT / "meta/10K_meta"

    # Ensure directories exist
    gpt_raw.mkdir(parents=True, exist_ok=True)

    # Read the CSV file
    df = pd.read_csv(MIR_CSV)

    with Pool(64) as pool:
        pool.map(process_file, [row for _, row in df.iterrows()])

    # for debugging
    # for _, row in df.iterrows():
    #     process_file(row)
    # Fail case:  7040148073490466817  !!!!!
    
    build_final_csv()
