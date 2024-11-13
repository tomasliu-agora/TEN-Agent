#
#
# Agora Real Time Engagement
# Created by Tomas Liu in 2024-10.
# Copyright (c) 2024 Agora IO. All rights reserved.
#
#
import threading
from datetime import datetime
import base64
import requests
import json
import httpx

from queue import Queue
from typing import Iterator, Any, List

from ten import (
    AudioFrame,
    AudioFrameDataFmt,
    VideoFrame,
    Extension,
    TenEnv,
    Cmd,
    StatusCode,
    CmdResult,
    Data,
)
from .log import logger

PROPERTY_GROUP_ID = "group_id"
PROPERTY_API_KEY = "api_key"
PROPERTY_SAMPLE_RATE = "sample_rate"
PROPERTY_MODEL = "model"
PROPERTY_VOICE_ID = "voice_id"
PROPERTY_DUMP = "dump"


class MiniMaxExtension(Extension):
    ten_env: TenEnv = None

    group_id: str = ""
    api_key: str = ""
    model: str = "speech-01-turbo"
    voice_id: str = "male-qn-qingse"
    sample_rate: int = 32000

    dump: bool = False

    thread: threading.Thread = None
    queue = Queue()

    stopped: bool = False
    outdate_ts = datetime.now()
    mutex = threading.Lock()

    client: httpx.Client = None

    def on_init(self, ten_env: TenEnv) -> None:
        logger.info("MiniMaxExtension on_init")
        self.ten_env = ten_env
        ten_env.on_init_done()

    def on_start(self, ten_env: TenEnv) -> None:
        logger.info("MiniMaxExtension on_start")

        try:
            self.group_id = ten_env.get_property_string(PROPERTY_GROUP_ID)
        except Exception as err:
            logger.info(
                f"GetProperty required {PROPERTY_GROUP_ID} failed, err: {err}")
            return

        try:
            self.api_key = ten_env.get_property_string(PROPERTY_API_KEY)
        except Exception as err:
            logger.info(
                f"GetProperty required {PROPERTY_API_KEY} failed, err: {err}")

        try:
            self.sample_rate = ten_env.get_property_int(
                PROPERTY_SAMPLE_RATE)
        except Exception as err:
            logger.info(
                f"GetProperty required {PROPERTY_SAMPLE_RATE} failed, err: {err}")

        try:
            self.model = ten_env.get_property_string(
                PROPERTY_MODEL)
        except Exception as err:
            logger.info(
                f"GetProperty required {PROPERTY_MODEL} failed, err: {err}")

        try:
            self.voice_id = ten_env.get_property_string(PROPERTY_VOICE_ID)
        except Exception as err:
            logger.info(
                f"GetProperty required {PROPERTY_VOICE_ID} failed, err: {err}")

        try:
            self.dump = ten_env.get_property_bool(PROPERTY_DUMP)
        except Exception as err:
            logger.info(
                f"GetProperty required {PROPERTY_DUMP} failed, err: {err}")

        self.client = httpx.Client(timeout=httpx.Timeout(5))

        self.thread = threading.Thread(target=self.loop)
        self.thread.start()

        ten_env.on_start_done()

    def on_stop(self, ten_env: TenEnv) -> None:
        logger.info("MiniMaxExtension on_stop")

        self.stopped = True
        self._flush()
        self.queue.put(None)
        if self.thread:
            self.thread.join()
            self.thread = None

        if self.client:
            self.client.close()
            self.client = None

        ten_env.on_stop_done()

    def loop(self) -> None:
        while not self.stopped:
            entry = self.queue.get()
            if entry is None:
                return

            try:
                ts, text = entry
                if self._need_interrupt(ts):
                    continue
                self._call_tts_stream(ts, text)
            except Exception as e:
                logger.exception(f"Failed to handle entry, err {e}")

    def on_cmd(self, ten_env: TenEnv, cmd: Cmd) -> None:
        cmd_name = cmd.get_name()
        logger.info("on_cmd name {}".format(cmd_name))

        if cmd_name == "flush":
            self._flush()

            out_cmd = Cmd.create("flush")
            ten_env.send_cmd(
                out_cmd, lambda ten, result: logger.info(
                    "send_cmd flush done"),
            )

        cmd_result = CmdResult.create(StatusCode.OK)
        ten_env.return_result(cmd_result, cmd)

    def on_data(self, ten_env: TenEnv, data: Data) -> None:
        logger.debug("on_data")

        try:
            text = data.get_property_string("text")
        except Exception as e:
            logger.warning(
                f"on_data get_property_string text error: {e}")
            return

        if len(text) == 0:
            logger.debug("on_data text is empty, ignored")
            return

        logger.info(f"OnData input text: [{text}]")

        self.queue.put((datetime.now(), text))

    def on_audio_frame(self, ten_env: TenEnv, audio_frame: AudioFrame) -> None:
        pass

    def on_video_frame(self, ten_env: TenEnv, video_frame: VideoFrame) -> None:
        pass

    def _need_interrupt(self, ts: datetime.time) -> bool:
        with self.mutex:
            return self.outdate_ts > ts

    def _call_tts_stream(self, ts: datetime, text: str) -> Iterator[bytes]:
        payload = {
            "model": self.model,
            "text": text,
            "stream": True,
            "voice_setting": {
                "voice_id": self.voice_id,
                "speed": 1.0,
                "vol": 1.0,
                "pitch": 0
            },
            # "pronunciation_dict": {
            #     "tone": [
            #         "处理/(chu3)(li3)", "危险/dangerous"
            #     ]
            # },
            "audio_setting": {
                "audio_sample_rate": self.sample_rate,
                # "bitrate": 128000,
                "format": "pcm",
                "channel": 1
            }
        }

        url = "https://api.minimax.chat/v1/t2a_v2?GroupId=" + self.group_id
        headers = {
            'accept': 'application/json, text/plain, */*',
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

        start_time = datetime.now()
        logger.info(f"start request, url {url}")
        # response = requests.post(url, headers=headers, json=payload, stream=True, timeout=5)
        # logger.info(f"Get response, trace-id: {response.headers.get('Trace-Id')}, cost_time {self._duration_in_ms_since(start_time)}ms")
        ttfb = None
        i = 0
        # with httpx.Client(timeout=httpx.Timeout(5)) as client:
        try:
            # 发送 POST 请求
            # with self.client.stream("POST", url, headers=headers, json=payload) as response:
            with requests.request("POST", url, stream=True, headers=headers, data=json.dumps(payload)) as response:
                # with self.client.post(url, headers=headers, json=payload) as response:
                trace_id = ""
                alb_receive_time = ""
                try:
                    trace_id = response.headers.get("Trace-Id")
                except:
                    logger.warning("Get response, no Trace-Id")
                try:
                    alb_receive_time = response.headers.get("alb_receive_time")
                except:
                    logger.warning("Get response, no alb_receive_time")
                logger.info(
                    f"Get response trace-id: {trace_id}, alb_receive_time: {alb_receive_time}, cost_time {self._duration_in_ms_since(start_time)}ms"
                )

                response.raise_for_status()  # 检查响应状态

                # for chunk in response.iter_raw():
                for chunk in (response.raw):

                    if self._need_interrupt(ts):
                        logger.warning(f"trace-id: {trace_id}, interrupted")
                        break

                    if chunk:
                        if chunk[:5] == b'data:':
                            logger.info(f"chunk len {len(chunk)}")
                            data = json.loads(chunk[5:])
                            if "data" in data and "extra_info" not in data:
                                if "audio" in data["data"]:
                                    audio = data["data"]['audio']
                                    if audio is not None and audio != '\n':
                                        decoded_hex = bytes.fromhex(audio)
                                        self._send_audio_out(decoded_hex)

                                    if not ttfb:
                                        ttfb = self._duration_in_ms_since(
                                            start_time)
                                        logger.info(
                                            f"trace-id: {trace_id}, ttfb {ttfb}ms")
                        else:
                            logger.warning(f"invalid chunk {chunk}")

                    i += 1

        except httpx.TimeoutException:
            logger.warning("http timeout")
        except httpx.HTTPStatusError as e:
            logger.warning(f"http status error: {e}")
        except httpx.RequestError as e:
            logger.warning(f"http request error: {e}")
        except Exception as e:
            logger.warning(f"unknown err {e}")
        finally:
            logger.info(
                f"http loop done, cost_time {self._duration_in_ms_since(start_time)}ms")

    def _send_audio_out(self, audio_data: bytearray) -> None:
        self._dump_audio_if_need(audio_data, "out")

        try:
            f = AudioFrame.create("pcm_frame")
            f.set_sample_rate(self.sample_rate)
            f.set_bytes_per_sample(2)
            f.set_number_of_channels(1)
            f.set_data_fmt(AudioFrameDataFmt.INTERLEAVE)
            f.set_samples_per_channel(len(audio_data) // 2)
            f.alloc_buf(len(audio_data))
            buff = f.lock_buf()
            buff[:] = audio_data
            f.unlock_buf(buff)
            self.ten_env.send_audio_frame(f)
        except:
            logger.exception("Error send audio frame")

    def _flush(self) -> None:
        with self.mutex:
            self.outdate_ts = datetime.now()
        while not self.queue.empty():
            self.queue.get()

    def _dump_audio_if_need(self, buf: bytearray, suffix: str) -> None:
        if not self.dump:
            return

        with open("{}_{}.pcm".format("minimax_tts", suffix), "ab") as dump_file:
            dump_file.write(buf)

    def _duration_in_ms(self, start: datetime, end: datetime) -> int:
        return int((end - start).total_seconds() * 1000)

    def _duration_in_ms_since(self, start: datetime) -> int:
        return self._duration_in_ms(start, datetime.now())
