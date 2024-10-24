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
import pathlib
import os

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

PROPERTY_TOKEN = "token"
PROPERTY_PROMPT = "prompt"
PROPERTY_IN_SAMPLE_RATE = "in_sample_rate"
PROPERTY_OUT_SAMPLE_RATE = "out_sample_rate"
PROPERTY_MODEL = "model"
PROPERTY_VOICE_MODEL = "voice_model"
PROPERTY_VOICE_ID = "voice_id"
PROPERTY_DUMP = "dump"

class MiniMaxExtension(Extension):
    ten_env:TenEnv = None
    token:str = ""
    thread:threading.Thread = None
    queue = Queue()

    stopped: bool = False
    outdate_ts = datetime.now()
    mutex = threading.Lock()
    history: List[str] = []
    max_history: int = 10
    remote_stream_id: int = 0
    transcript: str = ""

    model:str = "abab6.5s-chat"
    voice_model:str = "speech-01-turbo-240228"
    voice_id:str = "female-tianmei"
    prompt:str = "You are a voice assistant who talks in a conversational way and can chat with me like my friends. I will speak to you in English or Chinese, and you will answer in the corrected and improved version of my text with the language I use. Don’t talk like a robot, instead I would like you to talk like a real human with emotions. I will use your answer for text-to-speech, so don’t return me any meaningless characters. I want you to be helpful, when I’m asking you for advice, give me precise, practical and useful advice instead of being vague. When giving me a list of options, express the options in a narrative way instead of bullet points."
    in_sample_rate:int = 16000
    out_sample_rate:int = 32000

    dump: bool = False

    client: httpx.Client = None

    def on_init(self, ten_env: TenEnv) -> None:
        logger.info("MiniMaxExtension on_init")
        self.ten_env = ten_env
        ten_env.on_init_done()

    def on_start(self, ten_env: TenEnv) -> None:
        logger.info("MiniMaxExtension on_start")

        try:
            self.token = ten_env.get_property_string(PROPERTY_TOKEN)
        except Exception as err:
            logger.info(
                f"GetProperty required {PROPERTY_TOKEN} failed, err: {err}")
            return
        
        try:
            self.prompt = ten_env.get_property_string(PROPERTY_PROMPT)
        except Exception as err:
            logger.info(
                f"GetProperty required {PROPERTY_PROMPT} failed, err: {err}")

        try:
            self.in_sample_rate = ten_env.get_property_int(PROPERTY_IN_SAMPLE_RATE)
        except Exception as err:
            logger.info(
                f"GetProperty required {PROPERTY_IN_SAMPLE_RATE} failed, err: {err}")

        try:
            self.out_sample_rate = ten_env.get_property_int(PROPERTY_OUT_SAMPLE_RATE)
        except Exception as err:
            logger.info(
                f"GetProperty required {PROPERTY_OUT_SAMPLE_RATE} failed, err: {err}")

        try:
            self.model = ten_env.get_property_string(PROPERTY_MODEL)
        except Exception as err:
            logger.info(
                f"GetProperty required {PROPERTY_MODEL} failed, err: {err}")

        try:
            self.voice_model = ten_env.get_property_string(PROPERTY_VOICE_MODEL)
        except Exception as err:
            logger.info(
                f"GetProperty required {PROPERTY_VOICE_MODEL} failed, err: {err}")

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
                ts, buff = entry
                if self._need_interrupt(ts):
                    continue
                self._complete_with_history(ts, buff)
            except:
                logger.exception(f"Failed to handle entry")

    def on_deinit(self, ten_env: TenEnv) -> None:
        logger.info("MiniMaxExtension on_deinit")
        ten_env.on_deinit_done()

    def on_cmd(self, ten_env: TenEnv, cmd: Cmd) -> None:
        cmd_name = cmd.get_name()
        logger.info("on_cmd name {}".format(cmd_name))

        if cmd_name == "flush":
            self._flush()

            out_cmd = Cmd.create("flush")
            ten_env.send_cmd(
                out_cmd, lambda ten, result: logger.info("send_cmd flush done"),
            )
        # elif cmd_name == "on_user_joined":
        #     hello_cn_file = pathlib.Path(__file__).parent.absolute().joinpath("hello_cn.pcm")
        #     with open(hello_cn_file, "rb") as f:
        #         content = f.read()
        #         self.queue.put((datetime.now(), content))

        cmd_result = CmdResult.create(StatusCode.OK)
        ten_env.return_result(cmd_result, cmd)

    def on_data(self, ten_env: TenEnv, data: Data) -> None:
        pass

    def on_audio_frame(self, ten_env: TenEnv, audio_frame: AudioFrame) -> None:
        # Must be after vad
        try:
            ts = datetime.now()

            stream_id = audio_frame.get_property_int("stream_id")
            self.remote_stream_id = stream_id

            while not self.queue.empty():
                self.queue.get()
            
            frame_buf = audio_frame.get_buf()
            logger.info(f"on audio frame {len(frame_buf)} {stream_id}")
            self._dump_audio_if_need(frame_buf, "in")
            self.queue.put((ts, frame_buf))
        except:
            logger.exception(f"MiniMaxExtension on audio frame failed")

    def on_video_frame(self, ten_env: TenEnv, video_frame: VideoFrame) -> None:
        pass

    def _need_interrupt(self, ts: datetime.time) -> bool:
        with self.mutex:
            return self.outdate_ts > ts
        
    def _complete_with_history(self, ts: datetime, buff: bytearray) -> Iterator[bytes]:
        messages = self._get_messages()
        messages.append({
            "role": "user",
            "content": [
                {
                    "type": "input_audio",
                    "input_audio": {
                        "data": base64.b64encode(buff).decode("utf-8"),
                        "format": "pcm",
                        "sample_rate": self.in_sample_rate,
                        "bit_depth": 16,
                        "channel": 1,
                        "encode": "base64"
                    }
                }
            ]})
        
        url = "https://api.minimax.chat/v1/text/chatcompletion_v2"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        payload = {
            "model": self.model,
            "messages": messages,
            "tools": [],
            "tool_choice": "none",
            "stream": True,
            "stream_options": { # 开启语音输出
                "speech_output": True
            },
            "voice_setting":{
                "model": self.voice_model,
                "voice_id": self.voice_id
            },
            "audio_setting": {
                "sample_rate": self.out_sample_rate,
                "format": "pcm",
                "channel": 1,
                "encode": "base64"
            },
            "tools":[
                {
                    "type":"web_search"
                }
            ],
            "max_tokens": 1024,
            "temperature": 0.8,
            "top_p": 0.95
        }
        
        start_time = datetime.now()
        logger.info(f"start request, data len {len(buff)}")
        # response = requests.post(url, headers=headers, json=payload, stream=True, timeout=5)
        # logger.info(f"Get response, trace-id: {response.headers.get('Trace-Id')}, cost_time {self._duration_in_ms_since(start_time)}ms")
        user_transcript_ttfb = None
        assistant_transcript_ttfb = None
        assistant_audio_ttfb = None
        self.transcript = ""
        i = 0
        # with httpx.Client(timeout=httpx.Timeout(5)) as client:
        try:
            # 发送 POST 请求
            with self.client.stream("POST", url, headers=headers, json=payload) as response:
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

                for line in response.iter_lines():
                    # logger.info(f"-> line {line}")
                    if self._need_interrupt(ts):
                        logger.warning(f"trace-id: {trace_id}, interrupted")
                        if self.transcript:
                            self.transcript += "[interrupted]"
                            self._append_message("assistant", self.transcript)
                            self._send_transcript("", "assistant", True)
                        break

                    if not line.startswith("data:"):
                        logger.warning(f"ignore line {len(line)}")
                        continue

                    i+=1

                    resp = json.loads(line.strip("data:"))
                    if resp.get("choices") and resp["choices"][0].get("delta"):
                        delta = resp["choices"][0]["delta"]
                        if delta.get("role") == "assistant":
                            if delta.get("content"):
                                content = delta['content']
                                self.transcript += content
                                logger.info(f"[sse] data chunck-{i} get assistant transcript {content}")
                                self._send_transcript(content, "assistant", False)
                                if not assistant_transcript_ttfb:
                                    assistant_transcript_ttfb = self._duration_in_ms_since(start_time)
                                    logger.info(f"trace-id: {trace_id}, assistant_transcript_ttfb {assistant_transcript_ttfb}ms")
                            if delta.get("audio_content") and delta["audio_content"] != "":
                                logger.info(f"[sse] data chunck-{i} get audio_content")
                                base64_str = delta["audio_content"]
                                # with open(f"minimax_v2v_data_{i}.txt", "a") as f:
                                #     f.write(base64_str)
                                buff = base64.b64decode(base64_str)
                                self._send_audio_out(buff)
                                if not assistant_audio_ttfb:
                                    assistant_audio_ttfb = self._duration_in_ms_since(start_time)
                                    logger.info(f"trace-id: {trace_id}, assistant_audio_ttfb {assistant_audio_ttfb}ms")
                            if delta.get("tool_calls"):
                                logger.info(f"ignore tool call {delta}")
                                continue
                        if delta.get("role") == "user":
                            self._send_transcript(delta['content'], "user", True)
                            if not user_transcript_ttfb:
                                user_transcript_ttfb = self._duration_in_ms_since(start_time)
                                logger.info(f"trace-id: {trace_id}, user_transcript_ttfb {user_transcript_ttfb}ms")

        except httpx.TimeoutException:
            logger.warning("http timeout")
        except httpx.HTTPStatusError as e:
            logger.warning(f"http status error: {e}")
        except httpx.RequestError as e:
            logger.warning(f"http request error: {e}")
        finally:
            logger.info(f"http loop done, cost_time {self._duration_in_ms_since(start_time)}ms")
            if self.transcript:
                self._append_message("assistant", self.transcript)
                self._send_transcript("", "assistant", True)

        # for line in response.iter_lines(decode_unicode=True):
        #     if self._need_interrupt(ts):
        #         logger.warning("interrupted")
        #         if self.transcript:
        #             self.transcript += "[interrupted]"
        #             self._append_message("assistant", self.transcript)
        #             self._send_transcript("", "assistant", True)
        #         break

        #     if not line.startswith("data:"):
        #         logger.warning(f"ignore line {len(line)}")
        #         continue

        #     i+=1

        #     resp = json.loads(line.strip("data:"))
        #     if resp.get("choices") and resp["choices"][0].get("delta"):
        #         delta = resp["choices"][0]["delta"]
        #         if delta.get("role") == "assistant":
        #             if delta.get("content"):
        #                 content = delta['content']
        #                 self.transcript += content
        #                 logger.info(f"[sse] data chunck-{i} get assistant transcript {content}")
        #                 self._send_transcript(content, "assistant", False)
        #             if delta.get("audio_content") and delta["audio_content"] != "":
        #                 logger.info(f"[sse] data chunck-{i} get audio_content")
        #                 base64_str = delta["audio_content"]
        #                 # with open(f"minimax_v2v_data_{i}.txt", "a") as f:
        #                 #     f.write(base64_str)
        #                 buff = base64.b64decode(base64_str)
        #                 self._send_audio_out(buff)
        #             if delta.get("tool_calls"):
        #                 logger.info(f"ignore tool call {delta}")
        #                 continue
        #         if delta.get("role") == "user":
        #             self._send_transcript(delta['content'], "user", True)

        # logger.info(f"Get response loop done, cost_time {self._duration_in_ms_since(start_time)}ms")
        # if self.transcript:
        #     self._append_message("assistant", self.transcript)
        #     self._send_transcript("", "assistant", True)

    def _get_messages(self) -> List[Any]:
        messages = []
        if len(self.prompt) > 0:
            messages.append({"role": "system", "content": self.prompt})
        self.mutex.acquire()
        try:
            for h in self.history:
                messages.append(h)
        finally:
            self.mutex.release()
        return messages

    def _append_message(self, role: str, content: str) -> None:
        self.mutex.acquire()
        try:
            logger.info(f"append history {content}")
            self.history.append({"role": role, "content": content})
            if len(self.history) > self.max_history:
                self.history = self.history[1:]
        finally:
            self.mutex.release()

    def _send_audio_out(self, audio_data:bytearray) -> None:
        self._dump_audio_if_need(audio_data, "out")
        
        try:
            f = AudioFrame.create("pcm_frame")
            f.set_sample_rate(self.out_sample_rate)
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

    def _send_transcript(self, content:str, role:str, is_final:bool) -> None:
        stream_id = self.remote_stream_id if role == "user" else 0
        try:
            d = Data.create("text_data")
            d.set_property_string("text", content)
            d.set_property_bool("end_of_segment", is_final)
            d.set_property_string("role", role)
            d.set_property_int("stream_id", stream_id)
            logger.info(
                f"send transcript text [{content}] {stream_id} is_final {is_final} end_of_segment {is_final} role {role}")
            self.ten_env.send_data(d)
        except:
            logger.exception(
                f"Error send text data {role}: {content} {is_final}")
    
    def _flush(self) -> None:
        with self.mutex:
            self.outdate_ts = datetime.now()
        while not self.queue.empty():
            self.queue.get()

    def _dump_audio_if_need(self, buf: bytearray, suffix: str) -> None:
        if not self.dump:
            return

        with open("{}_{}.pcm".format("minimax_v2v", suffix), "ab") as dump_file:
            dump_file.write(buf)

    def _duration_in_ms(self, start: datetime, end: datetime) -> int:
        return int((end - start).total_seconds() * 1000)

    def _duration_in_ms_since(self, start: datetime) -> int:
        return self._duration_in_ms(start, datetime.now())
