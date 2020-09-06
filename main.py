import argparse
import asyncio
import concurrent.futures
import operator
import os
import sys
import time
import urllib.parse
from concurrent.futures.thread import ThreadPoolExecutor
import multiprocessing
from os import listdir
from os.path import isfile, join, isdir
from pathlib import Path

import numpy as np
import requests
import json
import dropbox
from dropbox import DropboxOAuth2FlowNoRedirect
import subprocess

from dropbox.sharing import SharedLinkSettings

LF_URL = "https://lethflix.ew.r.appspot.com/"
LF_LIBRARY_PATH = Path("C:\\Users\\peter\\Dropbox\\Apps\\lethflix\\library1")

# parse config file
with open('./config.json') as f:
    cfg = json.load(f)
    LF_ADMIN_TOKEN = cfg["lethflix_admin_token"]
    DROPBOX_APP_KEY = cfg["dropbox_app_key"]
    DROPBOX_APP_SECRET = cfg["dropbox_app_secret"]

# connect to dropbox
"""auth_flow = DropboxOAuth2FlowNoRedirect(DROPBOX_APP_KEY, DROPBOX_APP_SECRET)
authorize_url = auth_flow.start()
print("1. Go to: " + authorize_url)
print("2. Click \"Allow\" (you might have to log in first).")
print("3. Copy the authorization code.")
auth_code = input("Enter the authorization code here: ").strip()
try:
    oauth_result = auth_flow.finish(auth_code)
except Exception as e:
    print('Error: %s' % (e,))
    exit(1)
DBX = dropbox.Dropbox(oauth2_access_token=oauth_result.access_token)
DBX.users_get_current_account()
print("dropbox client OK")"""


def get_source_path():
    """ Retrieves the file system path to the media source and parses it """
    parser = argparse.ArgumentParser()
    parser.add_argument("source",
                        type=str,
                        help="The location of the media to be imported. If not specified, and inserted disc will be "
                             "searched for and processed.")
    parser.add_argument(  # not a requirement
        '--skip', '--s', action='store_true', help='skip encoding and only do linking (requires an '
                                                   'already-encoded hls stream')
    args = parser.parse_args()

    path = Path(args.source)
    skip_encoding = args.skip
    assert os.path.exists(path), "provided path \"%s\" is not valid" % path
    print("provided path OK")
    return (path, skip_encoding)


def connect_dropbox_client():
    pass


def network_check():
    """ Checks server availability before starting the encoding """
    print("checking server status...")
    try:
        rh = requests.head(LF_URL, timeout=10)
        assert rh.status_code == 200, "server responded with none-200 status code"
        print("lethflix server OK")
        token_check = requests.post(LF_URL + "verify", params={'token': LF_ADMIN_TOKEN})
        assert token_check.status_code == 200, "admin token was rejected by the server"
        print("admin token OK")
        # connect_dropbox_client()
    except requests.exceptions.ConnectTimeout as e:
        assert False, "network check failed: %s" % e  # needless except and assert here to make output cleaner


def get_most_recent_upload():
    """ Retrieves the most recent upload from the server, in case related media is uploaded in succession """
    print("retrieving most recent upload...")
    try:
        r = requests.get(LF_URL + "mostrecentupload")
        assert r.status_code == 200, "server responded with none-200 status code"
        print("successfully retrieved most recent upload with id: %s" % r.json()["id"])
        return r.json()
    except requests.exceptions.ConnectTimeout as e:
        assert False, "get_most_recent_upload failed: %s" % e


def get_media_object(content_id):
    """ Retrieves the media object associated with the given id from the server """
    print("retrieving media object with id: %s..." % content_id)
    try:
        r = requests.get(LF_URL + "getcontent")
        assert r.status_code == 200, "server responded with none-200 status code when requesting id: %s" % content_id
        print("successfully retrieved media object with id: %s" % content_id)
        return r.json()
    except requests.exceptions.ConnectTimeout as e:
        assert False, "get_most_recent_upload failed: %s" % e


def compose_media_object():
    """ Collects content-related meta data in a dictionary """
    title = input("Title: ")
    director = input("Director: ")
    starring = input("Starring: (separated by \",\") ")
    description = input("Description: ")
    tags = input("Tags: (separated by \",\") ")
    intro_start = input("Intro start timestamp: (mm:ss) ")
    intro_stop = input("Intro stop timestamp: (mm:ss) ")
    outro_start = input("Outro start timestamp: (mm:ss) ")
    outro_stop = input("Outro stop timestamp: (mm:ss) ")

    previous_id = input("Previous media id: (\"y\" will autofill) ")
    if previous_id == "y":
        recent_upload = get_most_recent_upload()
        is_correct = input("Most recent upload is %s (y/n): " % recent_upload.title)
        if is_correct == "y":
            previous_id = recent_upload["previous_id"]
        else:
            exit(1)
    elif previous_id != "":
        if get_media_object(previous_id):  # test the given id to be sure
            previous_media_object = get_media_object(previous_id)

    # return initial media object
    return {
        "title": title,
        "director": director,
        "starring": starring.split(', '),
        "description": description,
        "tags": tags.split(', '),
        "triggers": {
            "intro_start": intro_start,
            "intro_stop": intro_stop,
            "outro_start": outro_start,
            "outro_stop": outro_stop
        }
    }


def get_ordered_media(path):
    """ Bundles all media at the specified location into a list, with the main content as the last index """
    files_in_path = [f for f in listdir(path) if isfile(join(path, f)) and f[-4:] == ".mkv"]
    subtitles_in_path = [f for f in listdir(path) if isfile(join(path, f)) and f[-4:] == ".srt"]
    assert len(files_in_path) >= 1, "found no .mkv files in provided path"
    print("found the following %s content file(s): %s" % (len(files_in_path), files_in_path))
    if len(subtitles_in_path) > 0:
        print("found the following %s subtitle file(s): %s" % (len(subtitles_in_path), subtitles_in_path))
    else:
        print("found no subtitle files in path")
    largest_file = ""
    for f in files_in_path:
        file_path = join(path, f)
        if largest_file == "" or os.path.getsize(file_path) > os.path.getsize(largest_file):
            largest_file = file_path

    result = [join(path, f) for f in files_in_path if join(path, f) != largest_file]
    result.insert(len(result), largest_file)
    return result, subtitles_in_path


def encode_media(parent_path, input_path, target_path):
    """ Performs the actual encoding of the source media, along with the conversion to HLS and DropBox linking """
    print("encode_media recieved: %s (parent), %s (input), %s (target)" % (parent_path, input_path, target_path))
    # TODO: figure out ratio between video bitrate, maxrate, and bufsize...
    encodings_old = {
        "416x234@200k": ["ffmpeg", "-hide_banner", "-i", target_path, "-vf",
                         "scale=trunc(oh*a/2)*2:234", "-c:a", "aac", "-ac", "2",
                         "-c:v", "libx264", "-pixel_format", "yuv420p", "-profile:v", "baseline", "-crf", "20",
                         "-flags", "+cgop", "-sc_threshold", "0", "-g", "150", "-keyint_min", "150", "-r", "12",
                         "-hls_time", "4", "-hls_playlist_type", "vod", "-b:v", "200k", "-maxrate", "400k",
                         "-bufsize", "800k", "-b:a", "64k", "-pix_fmt", "yuv420p", "-hls_segment_filename",
                         join(parent_path, input_path, input_path + "_%03d.ts"),
                         join(parent_path, input_path, "manifest.m3u8")],
        "480x270@400k": ["ffmpeg", "-hide_banner", "-i", target_path, "-vf",
                         "scale=trunc(oh*a/2)*2:270", "-c:a", "aac", "-ac", "2",
                         "-c:v", "libx264", "-pixel_format", "yuv420p", "-profile:v", "baseline", "-crf", "20",
                         "-flags", "+cgop", "-sc_threshold", "0", "-g", "150", "-keyint_min", "150", "-r", "15",
                         "-hls_time", "4", "-hls_playlist_type", "vod", "-b:v", "400k", "-maxrate", "600k",
                         "-bufsize", "1200k", "-b:a", "64k", "-pix_fmt", "yuv420p", "-hls_segment_filename",
                         join(parent_path, input_path, input_path + "_%03d.ts"),
                         join(parent_path, input_path, "manifest.m3u8")],
        "640x360@600k": ["ffmpeg", "-hide_banner", "-i", target_path, "-vf",
                         "scale=trunc(oh*a/2)*2:360", "-c:a", "aac", "-ac", "2",
                         "-c:v", "libx264", "-pixel_format", "yuv420p", "-profile:v", "baseline", "-crf", "20",
                         "-flags", "+cgop", "-sc_threshold", "0", "-g", "150", "-keyint_min", "150", "-r", "30",
                         "-hls_time", "4", "-hls_playlist_type", "vod", "-b:v", "600k", "-maxrate", "800k",
                         "-bufsize", "1600k", "-b:a", "64k", "-pix_fmt", "yuv420p", "-hls_segment_filename",
                         join(parent_path, input_path, input_path + "_%03d.ts"),
                         join(parent_path, input_path, "manifest.m3u8")],
        "640x360@1200k": ["ffmpeg", "-hide_banner", "-i", target_path, "-vf",
                          "scale=trunc(oh*a/2)*2:360", "-c:a", "aac", "-ac", "2",
                          "-c:v", "libx264", "-pixel_format", "yuv420p", "-profile:v", "baseline", "-crf", "20",
                          "-flags", "+cgop", "-sc_threshold", "0", "-g", "150", "-keyint_min", "150", "-r", "30",
                          "-hls_time", "4", "-hls_playlist_type", "vod", "-b:v", "1200k", "-maxrate", "1400k",
                          "-bufsize", "2600k", "-b:a", "96k", "-pix_fmt", "yuv420p", "-hls_segment_filename",
                          join(parent_path, input_path, input_path + "_%03d.ts"),
                          join(parent_path, input_path, "manifest.m3u8")],
        "960x540@3500k": ["ffmpeg", "-hide_banner", "-i", target_path, "-vf",
                          "scale=trunc(oh*a/2)*2:540", "-c:a", "aac", "-ac", "2",
                          "-c:v", "libx264", "-pixel_format", "yuv420p", "-profile:v", "main", "-crf", "20",
                          "-flags", "+cgop", "-sc_threshold", "0", "-g", "150", "-keyint_min", "150", "-r", "30",
                          "-hls_time", "4", "-hls_playlist_type", "vod", "-b:v", "3500k", "-maxrate", "3700k",
                          "-bufsize", "7400k", "-b:a", "64k", "-pix_fmt", "yuv420p", "-hls_segment_filename",
                          join(parent_path, input_path, input_path + "_%03d.ts"),
                          join(parent_path, input_path, "manifest.m3u8")],
        "1280x720@5000k": ["ffmpeg", "-hide_banner", "-i", target_path, "-vf",
                           "scale=trunc(oh*a/2)*2:720", "-c:a", "aac", "-ac", "2",
                           "-c:v", "libx264", "-pixel_format", "yuv420p", "-profile:v", "main", "-crf", "20",
                           "-flags", "+cgop", "-sc_threshold", "0", "-g", "150", "-keyint_min", "150", "-r", "30",
                           "-hls_time", "4", "-hls_playlist_type", "vod", "-b:v", "5000k", "-maxrate", "5200k",
                           "-bufsize", "10400k", "-b:a", "128k", "-pix_fmt", "yuv420p", "-hls_segment_filename",
                           join(parent_path, input_path, input_path + "_%03d.ts"),
                           join(parent_path, input_path, "manifest.m3u8")],
        "1280x720@6500k": ["ffmpeg", "-hide_banner", "-i", target_path, "-vf",
                           "scale=trunc(oh*a/2)*2:720", "-c:a", "aac", "-ac", "2",
                           "-c:v", "libx264", "-pixel_format", "yuv420p", "-profile:v", "main", "-crf", "20",
                           "-flags", "+cgop", "-sc_threshold", "0", "-g", "150", "-keyint_min", "150", "-r", "30",
                           "-hls_time", "4", "-hls_playlist_type", "vod", "-b:v", "6500k", "-maxrate", "6700k",
                           "-bufsize", "13400k", "-b:a", "128k", "-pix_fmt", "yuv420p", "-hls_segment_filename",
                           join(parent_path, input_path, input_path + "_%03d.ts"),
                           join(parent_path, input_path, "manifest.m3u8")],
        "1920x1080@8500k": ["ffmpeg", "-hide_banner", "-i", target_path, "-vf",
                            "scale=trunc(oh*a/2)*2:1080", "-c:a", "aac", "-ac", "2",
                            "-c:v", "libx264", "-pixel_format", "yuv420p", "-profile:v", "high", "-crf", "20",
                            "-flags", "+cgop", "-sc_threshold", "0", "-g", "150", "-keyint_min", "150", "-r", "30",
                            "-hls_time", "4", "-hls_playlist_type", "vod", "-b:v", "8500k", "-maxrate", "8700k",
                            "-bufsize", "17400k", "-b:a", "128k", "-pix_fmt", "yuv420p", "-hls_segment_filename",
                            join(parent_path, input_path, input_path + "_%03d.ts"),
                            join(parent_path, input_path, "manifest.m3u8")]
    }
    encodings = {
        "480x270@365k": ["ffmpeg", "-hide_banner", "-i", target_path, "-vf",
                         "scale=trunc(oh*a/2)*2:270", "-c:a", "aac", "-ac", "2",
                         "-c:v", "libx264", "-pixel_format", "yuv420p", "-profile:v", "baseline", "-crf", "20",
                         "-flags", "+cgop", "-sc_threshold", "0", "-g", "150", "-keyint_min", "150", "-r", "15",
                         "-hls_time", "4", "-hls_playlist_type", "vod", "-b:v", "365k", "-maxrate", "465k",
                         "-bufsize", "1200k", "-b:a", "64k", "-pix_fmt", "yuv420p", "-hls_segment_filename",
                         join(parent_path, input_path, input_path + "_%03d.ts"),
                         join(parent_path, input_path, "manifest.m3u8")],
        "640x360@730k": ["ffmpeg", "-hide_banner", "-i", target_path, "-vf",
                         "scale=trunc(oh*a/2)*2:360", "-c:a", "aac", "-ac", "2",
                         "-c:v", "libx264", "-pixel_format", "yuv420p", "-profile:v", "baseline", "-crf", "20",
                         "-flags", "+cgop", "-sc_threshold", "0", "-g", "150", "-keyint_min", "150", "-r", "30",
                         "-hls_time", "4", "-hls_playlist_type", "vod", "-b:v", "730k", "-maxrate", "930k",
                         "-bufsize", "1600k", "-b:a", "64k", "-pix_fmt", "yuv420p", "-hls_segment_filename",
                         join(parent_path, input_path, input_path + "_%03d.ts"),
                         join(parent_path, input_path, "manifest.m3u8")],
        "960x540@2000k": ["ffmpeg", "-hide_banner", "-i", target_path, "-vf",
                          "scale=trunc(oh*a/2)*2:540", "-c:a", "aac", "-ac", "2",
                          "-c:v", "libx264", "-pixel_format", "yuv420p", "-profile:v", "main", "-crf", "20",
                          "-flags", "+cgop", "-sc_threshold", "0", "-g", "150", "-keyint_min", "150", "-r", "30",
                          "-hls_time", "4", "-hls_playlist_type", "vod", "-b:v", "2000k", "-maxrate", "2200k",
                          "-bufsize", "7400k", "-b:a", "64k", "-pix_fmt", "yuv420p", "-hls_segment_filename",
                          join(parent_path, input_path, input_path + "_%03d.ts"),
                          join(parent_path, input_path, "manifest.m3u8")],
        "1280x720@3000k": ["ffmpeg", "-hide_banner", "-i", target_path, "-vf",
                           "scale=trunc(oh*a/2)*2:720", "-c:a", "aac", "-ac", "2",
                           "-c:v", "libx264", "-pixel_format", "yuv420p", "-profile:v", "main", "-crf", "20",
                           "-flags", "+cgop", "-sc_threshold", "0", "-g", "150", "-keyint_min", "150", "-r", "30",
                           "-hls_time", "4", "-hls_playlist_type", "vod", "-b:v", "3000k", "-maxrate", "3200k",
                           "-bufsize", "10400k", "-b:a", "128k", "-pix_fmt", "yuv420p", "-hls_segment_filename",
                           join(parent_path, input_path, input_path + "_%03d.ts"),
                           join(parent_path, input_path, "manifest.m3u8")],
        "1920x1080@4500k": ["ffmpeg", "-hide_banner", "-i", target_path, "-vf",
                            "scale=trunc(oh*a/2)*2:1080", "-c:a", "aac", "-ac", "2",
                            "-c:v", "libx264", "-pixel_format", "yuv420p", "-profile:v", "high", "-crf", "20",
                            "-flags", "+cgop", "-sc_threshold", "0", "-g", "150", "-keyint_min", "150", "-r", "30",
                            "-hls_time", "4", "-hls_playlist_type", "vod", "-b:v", "4500k", "-maxrate", "4700k",
                            "-bufsize", "17400k", "-b:a", "128k", "-pix_fmt", "yuv420p", "-hls_segment_filename",
                            join(parent_path, input_path, input_path + "_%03d.ts"),
                            join(parent_path, input_path, "manifest.m3u8")],
        "1920x1080@8500k": ["ffmpeg", "-hide_banner", "-i", target_path, "-vf",
                            "scale=trunc(oh*a/2)*2:1080", "-c:a", "aac", "-ac", "2",
                            "-c:v", "libx264", "-pixel_format", "yuv420p", "-profile:v", "high", "-crf", "20",
                            "-flags", "+cgop", "-sc_threshold", "0", "-g", "150", "-keyint_min", "150", "-r", "30",
                            "-hls_time", "4", "-hls_playlist_type", "vod", "-b:v", "8500k", "-maxrate", "8700k",
                            "-bufsize", "17400k", "-b:a", "128k", "-pix_fmt", "yuv420p", "-hls_segment_filename",
                            join(parent_path, input_path, input_path + "_%03d.ts"),
                            join(parent_path, input_path, "manifest.m3u8")],
        "subtitles": ["ffmpeg", "-hide_banner", "-i", target_path[:-4] + ".srt", join(parent_path,
                                                                                      "subtitles.vtt")],
        "previews": ["ffmpeg", "-hide_banner", "-i", target_path, "-vf", "fps=0.01,scale=150:84", join(parent_path,
                                                                                                       "preview_images",
                                                                                                       "prev%d.jpg")]
    }
    print(join(parent_path, "subtitles.vtt"))
    print("encoding with the following ffmpeg command: %s" % encodings.get(input_path))
    return subprocess.Popen(encodings.get(input_path), stdout=subprocess.PIPE)


def upload_media_object(final_media_object):
    """ Uploads the media object as JSON to the server """
    pass  # ...


def get_dropbox_link(full_dropbox_path):
    data = {
        "path": full_dropbox_path,
        "settings": {
            "requested_visibility": "public",
            "audience": "public",
            "access": "viewer"
        }
    }
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer 6flYIJSHARAAAAAAAABPOIwR3n2U3Lw6YxBY5Tzx-lAs9RLvMFNyDNagz8qsWyTS"
    }
    result = ""
    retry = True
    sleep_time = 5
    while retry:
        try:
            r1 = requests.post("https://api.dropboxapi.com/2/sharing/create_shared_link_with_settings",
                               headers=headers,
                               data=json.dumps(data))
            result = r1.json()
            if "error" in r1.json() and "shared_link_already_exists" in r1.json()["error"][".tag"]:
                if "settings" in data:
                    del data["settings"]
                r1 = requests.post("https://api.dropboxapi.com/2/sharing/list_shared_links",
                                   headers=headers,
                                   data=json.dumps(data))
                result = r1.json()["links"][0]
            result = LF_URL + "tunnel?url=" + urllib.parse.quote(result["url"] + "&raw=1")  # add "raw" parameter
            # r2 = requests.get(result)
            # result = r2.url  # a redirect will yield the proper content url
            retry = False
        except:  # requests.exceptions.ConnectionError as e
            e = sys.exc_info()[0]
            print("error requesting %s: %s" % (full_dropbox_path, e))
            print("sleeping for %s seconds before retrying...." % sleep_time)
            time.sleep(sleep_time)
            if sleep_time < 100:
                sleep_time = round(sleep_time * 2)
    print(full_dropbox_path + " now available at: " + result)
    return result


def get_dropbox_link_async(full_dropbox_path, file_name, return_dict):
    result = get_dropbox_link(full_dropbox_path)
    return_dict[file_name] = result


def wait_for_dropbox_synchronization(media):
    """ wait for dropbox to finish synchronizing """
    print("waiting for dropbox to finish synchronizing...")
    synchronizing = True
    while synchronizing:
        time.sleep(5)
        r = requests.post("https://api.dropboxapi.com/2/files/list_folder",
                          headers={"Content-Type": "application/json",
                                   "Authorization": "Bearer 6flYIJSHARAAAAAAAABPOIwR3n2U3Lw6YxBY5Tzx-lAs9RLvMFNyDNagz8qsWyTS"},
                          data=json.dumps({"path": "/library1/%s" % media["title"]}))
        synchronizing = str(r.content[0:6]) != "b'{\"entr'"
    print("dropbox finished synchronizing, waiting 5 seconds before continuing...")
    time.sleep(5)  # sleep for extra seconds to give the dropbox server ample time to catch up


if __name__ == '__main__':
    source_path, skip_encoding = get_source_path()
    ordered_media = get_ordered_media(source_path)
    media_paths = ordered_media[0]
    subtitle_paths = ordered_media[1]
    network_check()

    media_object = compose_media_object()

    # setup content directory
    name = media_object["title"]
    if not skip_encoding:
        while os.path.exists(Path(join(str(LF_LIBRARY_PATH), name))):
            name = input("content title already exists, input new title: ")
    new_content_path = join(str(LF_LIBRARY_PATH), name)
    if not skip_encoding:
        subprocess.check_call(["mkdir", new_content_path], shell=True)
        subprocess.check_call(["mkdir", new_content_path + "\\main"], shell=True)
        for i in range(len(media_paths) - 1):
            subprocess.check_call(["mkdir", join(new_content_path, "bonus%s" % (i + 1))], shell=True)

    # setup HLS directories
    directories_in_path = [folder for folder in os.listdir(new_content_path)]
    if not skip_encoding:
        for folder in directories_in_path:
            stream_path_base = join(new_content_path, folder)
            subprocess.check_call(["mkdir", stream_path_base + "\\480x270@365k"], shell=True)
            subprocess.check_call(["mkdir", stream_path_base + "\\640x360@730k"], shell=True)
            subprocess.check_call(["mkdir", stream_path_base + "\\960x540@2000k"], shell=True)
            subprocess.check_call(["mkdir", stream_path_base + "\\1280x720@3000k"], shell=True)
            subprocess.check_call(["mkdir", stream_path_base + "\\1920x1080@4500k"], shell=True)
            subprocess.check_call(["mkdir", stream_path_base + "\\1920x1080@8500k"], shell=True)
            subprocess.check_call(["mkdir", stream_path_base + "\\preview_images"], shell=True)

        print("finished setup of file tree at: %s" % new_content_path)
        print("beginning encoding procedure...")

        # encode content
        encoding_processes = []
        for i in range(len(directories_in_path)):
            root_path = join(new_content_path, directories_in_path[i])
            for j in range(6):  # hardcoded number of different encodings
                # if i != 0 or j != 0: continue  # for debuggerino, removing this will open the floodgates
                target_subfolder = [subfolder for subfolder in os.listdir(root_path)][j]
                if target_subfolder[-1] != "k":
                    continue  # all folders containing video content end with a "k"
                encoding_processes.append(encode_media(root_path, target_subfolder, media_paths[i]))

            # generate subtitles
            # TODO: this will probably crash and burn eventually.
            encoding_processes.append(encode_media(root_path, "subtitles", media_paths[i]))
            # generate preview thumbnails
            encoding_processes.append(encode_media(root_path, "previews", media_paths[i]))

            # generate and link chapters
            print("generating chapterdata file...")
            chapters_raw = subprocess.check_output(
                ["ffprobe", "-i", media_paths[0], "-print_format", "json", "-show_chapters", "-loglevel", "error"],
                stderr=subprocess.STDOUT, universal_newlines=True)
            chapters_json = json.loads(chapters_raw)
            '''for line in iter(chapters_raw.splitlines()):
                m = re.match(r".*Chapter #(\d+:\d+): start (\d+\.\d+), end (\d+\.\d+).*", line)
                num = 0
                if m != None:
                    chapters.append({"name": m.group(1), "start": m.group(2), "end": m.group(3)})
                    num += 1
            print(chapters)'''
            # TODO: clean this up
            new_main_content_path = join(new_content_path, "main/")
            with open(join(new_main_content_path, "chapterdata.json"), "w+") as chapters_file:
                chapters_file.write(json.dumps(chapters_json))
            # wait_for_dropbox_synchronization(media_object)
            print("chapterdata file generated")

            # TODO: generate fallback content

        # wait for all encoding processes to finish
        for process in encoding_processes:
            process.communicate()

        print("finished encoding")

        wait_for_dropbox_synchronization(media_object)
    else:
        print("skipped encoding procedure")

    # link .ts video files with manifest files
    print("beginning linking procedure...")
    bandwidths = {}
    for folder1 in directories_in_path:
        root_path1 = join(new_content_path, folder1)
        for folder2 in listdir(root_path1):
            if folder2[-1] != "k" and folder2 != "preview_images":
                continue
            root_path2 = join(root_path1, folder2)
            files_in_path = [f for f in listdir(root_path2) if isfile(join(root_path2, f)) and f != "manifest.m3u8"]
            files_in_path.sort()
            bandwidths[folder1 + folder2] = 0
            processes = []
            manager = multiprocessing.Manager()
            share_links = manager.dict()
            print("allocating worker pool for batch %s..." % join(folder1, folder2))
            with multiprocessing.Pool(
                    processes=100) as pool:  # this might be a time-waster for content that is less than 5 minutes long
                for i in range(len(files_in_path)):
                    file = files_in_path[i]
                    # determine largest file size, used for calculating required bandwidth later
                    file_size = os.path.getsize(join(root_path2, file))
                    if file_size > bandwidths[folder1 + folder2]:
                        bandwidths[folder1 + folder2] = file_size
                    # fetch dropbox share link
                    # share_links.append(get_dropbox_link("/library/%s/%s/%s/%s" % (media_object["title"], folder1, folder2, file)))
                    dropbox_path = "/library1/%s/%s/%s/%s" % (media_object["title"], folder1, folder2, file)
                    try:
                        process = pool.apply_async(get_dropbox_link_async, (
                            dropbox_path, int(file[file.find("k_") + 2:file.find(".ts")]), share_links))
                    except:
                        process = pool.apply_async(get_dropbox_link_async, (
                            dropbox_path, int(file[-5]), share_links))
                    processes.append(process)

                print("awaiting batch %s..." % join(folder1, folder2))
                for process in processes:
                    process.wait()
                print("finished batch %s" % join(folder1, folder2))

            # sort dictionary in relation to keys and convert to list of values
            # share_links = sorted(share_links.items(), key=lambda elem: int(elem[0][elem.find("k_") + 2:elem[0].find(".ts")]))
            share_links = sorted(share_links.items(), key=operator.itemgetter(0))
            # print("share_links before: %s" % share_links)
            for i in range(len(share_links)):
                share_links[i] = share_links[i][1]
            # print("share_links after: %s" % share_links)

            if folder2[-1] == "k":
                # edit manifest.m3u8
                with open(join(root_path2, "manifest.m3u8"), "r+") as manifest:
                    lines = manifest.readlines()
                    i = 0
                    for j in range(len(lines)):
                        if lines[j][0] != "#":
                            lines[j] = share_links[i] + "\n"
                            i = i + 1
                    manifest.seek(0)
                    manifest.writelines(lines)
                    manifest.truncate()
            else:
                # save preview image links to .vtt formatted file
                with open(join(root_path2, "previewdata.vtt"), "w+") as file:
                    i = 0
                    file.write("WEBVTT 00:00:00.000 --> 00:00:10.000\n")
                    for j in range(len(share_links)):
                        file.write(share_links[j] + "\n\n")

                        def get_time(seconds):
                            return seconds % 60, seconds // 60 % 60, seconds // 60 // 60 % 60
                        seconds_total = i * 10 + 10
                        if j == len(share_links) - 1:
                            break
                        file.write("%s:%s:%s.000 --> %s:%s:%s.000\n" % (get_time(seconds_total)[2] if get_time(seconds_total)[2] > 9 else "0%s" % get_time(seconds_total)[2],
                                                                        get_time(seconds_total)[1] if get_time(seconds_total)[1] > 9 else "0%s" % get_time(seconds_total)[1],
                                                                        get_time(seconds_total)[0] if get_time(seconds_total)[0] > 9 else "0%s" % get_time(seconds_total)[0],
                                                                        get_time(seconds_total + 10)[2] if get_time(seconds_total + 10)[2] > 9 else "0%s" % get_time(seconds_total + 10)[2],
                                                                        get_time(seconds_total + 10)[1] if get_time(seconds_total + 10)[1] > 9 else "0%s" % get_time(seconds_total + 10)[1],
                                                                        get_time(seconds_total + 10)[0] if get_time(seconds_total + 10)[0] > 9 else "0%s" % get_time(seconds_total + 10)[0]))
                        i = i + 1

        # generate master playlist files
        playlists = {}
        for folder1 in directories_in_path:
            # link chapters file
            chapters_file_link = get_dropbox_link(
                "/library1/%s/%s/%s" % (media_object["title"], "main", "chapterdata.json"))
            # link subtitles file
            subtitles_file_link = get_dropbox_link(
                "/library1/%s/%s/%s" % (media_object["title"], "main", "subtitles.vtt"))
            thumbnails_file_link = get_dropbox_link(
                "/library1/%s/%s/%s/%s" % (media_object["title"], "main", "preview_images", "previewdata.vtt"))
            # TODO: link fallback content
            root_path1 = join(new_content_path, folder1)
            manifest_links = {}
            resolutions = []
            for folder2 in listdir(root_path1):
                if folder2[-1] == "k":
                    resolutions.append(folder2[:folder2.find("@")])
                    manifest_links[folder2] = get_dropbox_link(
                        "/library1/%s/%s/%s/%s" % (media_object["title"], folder1, folder2, "manifest.m3u8"))

            # compose the playlist.m3u8 file
            with open(join(root_path1, "playlist.m3u8"), "w+") as playlist:
                playlist.write("#EXTM3U\n")

                # declare chapterdata file
                playlist.write(
                    "#EXT-X-SESSION-DATA:DATA-ID=\"com.apple.hls.chapters\",URI=\"%s\"\n" % chapters_file_link)

                # declare manifest files, along with resolution and bandwidth requirements
                i = 0
                for resolution in manifest_links.keys():
                    playlist.write("#EXT-X-STREAM-INF:")  # intentionally without newline \n
                    bandwidth = bandwidths[folder1 + resolution]  # (.../ 4) * 8
                    resolution_string = resolutions[i]
                    playlist.write("BANDWIDTH=%s,RESOLUTION=%s\n" % (bandwidth, resolution_string))
                    playlist.write(manifest_links[resolution] + "\n")
                    i = i + 1

            wait_for_dropbox_synchronization(media_object)

            # save dropbox link to master playlist
            playlists[folder1] = get_dropbox_link("/library1/%s/%s/playlist.m3u8" % (media_object["title"], folder1))

        print("Final master playlists: " + str(playlists))
        media_object["urls"] = {
            'chapters': chapters_file_link,
            'subtitles': subtitles_file_link,
            'thumbnails': thumbnails_file_link,
            'video': playlists[folder1]
        }
        print("Final media object: " + str(media_object))

    # TODO: make it so that a script fail cleans up after itself

    # upload_media_object(final_media_object)
