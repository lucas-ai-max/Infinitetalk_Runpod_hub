import runpod
import os
import websocket
import base64
import json
import uuid
import logging
import urllib.request
import urllib.parse
import binascii
import subprocess
import librosa
import shutil

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def truncate_base64_for_log(base64_str, max_length=50):
    if not base64_str:
        return "None"
    if len(base64_str) <= max_length:
        return base64_str
    return f"{base64_str[:max_length]}... (총 {len(base64_str)} 문자)"


server_address = os.getenv("SERVER_ADDRESS", "127.0.0.1")
client_id = str(uuid.uuid4())


def download_file_from_url(url, output_path):
    """URL에서 파일을 다운로드하는 함수 (urllib 사용 - Supabase 호환)"""
    try:
        urllib.request.urlretrieve(url, output_path)
        file_size = os.path.getsize(output_path)
        logger.info(f"✅ URL에서 파일을 성공적으로 다운로드했습니다: {url} -> {output_path} ({file_size} bytes)")
        return output_path
    except Exception as e:
        logger.error(f"❌ 다운로드 중 오류 발생: {e}")
        raise Exception(f"다운로드 중 오류 발생: {e}")


def save_base64_to_file(base64_data, temp_dir, output_filename):
    try:
        decoded_data = base64.b64decode(base64_data)
        os.makedirs(temp_dir, exist_ok=True)
        file_path = os.path.abspath(os.path.join(temp_dir, output_filename))
        with open(file_path, "wb") as f:
            f.write(decoded_data)
        logger.info(f"✅ Base64 입력을 '{file_path}' 파일로 저장했습니다.")
        return file_path
    except (binascii.Error, ValueError) as e:
        logger.error(f"❌ Base64 디코딩 실패: {e}")
        raise Exception(f"Base64 디코딩 실패: {e}")


def process_input(input_data, temp_dir, output_filename, input_type):
    if input_type == "path":
        logger.info(f"📁 경로 입력 처리: {input_data}")
        return input_data
    elif input_type == "url":
        logger.info(f"🌐 URL 입력 처리: {input_data}")
        os.makedirs(temp_dir, exist_ok=True)
        file_path = os.path.abspath(os.path.join(temp_dir, output_filename))
        return download_file_from_url(input_data, file_path)
    elif input_type == "base64":
        logger.info(f"🔢 Base64 입력 처리")
        return save_base64_to_file(input_data, temp_dir, output_filename)
    else:
        raise Exception(f"지원하지 않는 입력 타입: {input_type}")


def queue_prompt(prompt, input_type="image", person_count="single"):
    url = f"http://{server_address}:8188/prompt"
    logger.info(f"Queueing prompt to: {url}")
    p = {"prompt": prompt, "client_id": client_id}
    data = json.dumps(p).encode("utf-8")

    logger.info(f"워크플로우 노드 수: {len(prompt)}")
    if input_type == "image":
        logger.info(f"이미지 노드(284) 설정: {prompt.get('284', {}).get('inputs', {}).get('image', 'NOT_FOUND')}")
    else:
        logger.info(f"비디오 노드(228) 설정: {prompt.get('228', {}).get('inputs', {}).get('video', 'NOT_FOUND')}")
    logger.info(f"오디오 노드(125) 설정: {prompt.get('125', {}).get('inputs', {}).get('audio', 'NOT_FOUND')}")
    logger.info(f"텍스트 노드(241) 설정: {prompt.get('241', {}).get('inputs', {}).get('positive_prompt', 'NOT_FOUND')}")
    if person_count == "multi":
        if "307" in prompt:
            logger.info(f"두 번째 오디오 노드(307) 설정: {prompt.get('307', {}).get('inputs', {}).get('audio', 'NOT_FOUND')}")
        elif "313" in prompt:
            logger.info(f"두 번째 오디오 노드(313) 설정: {prompt.get('313', {}).get('inputs', {}).get('audio', 'NOT_FOUND')}")

    req = urllib.request.Request(url, data=data)
    req.add_header("Content-Type", "application/json")

    try:
        response = urllib.request.urlopen(req)
        result = json.loads(response.read())
        logger.info(f"프롬프트 전송 성공: {result}")
        return result
    except urllib.error.HTTPError as e:
        logger.error(f"HTTP 에러 발생: {e.code} - {e.reason}")
        logger.error(f"응답 내용: {e.read().decode('utf-8')}")
        raise
    except Exception as e:
        logger.error(f"프롬프트 전송 중 오류: {e}")
        raise


def get_image(filename, subfolder, folder_type):
    url = f"http://{server_address}:8188/view"
    logger.info(f"Getting image from: {url}")
    data = {"filename": filename, "subfolder": subfolder, "type": folder_type}
    url_values = urllib.parse.urlencode(data)
    with urllib.request.urlopen(f"{url}?{url_values}") as response:
        return response.read()


def get_history(prompt_id):
    url = f"http://{server_address}:8188/history/{prompt_id}"
    logger.info(f"Getting history from: {url}")
    with urllib.request.urlopen(url) as response:
        return json.loads(response.read())


def get_videos(ws, prompt, input_type="image", person_count="single"):
    prompt_id = queue_prompt(prompt, input_type, person_count)["prompt_id"]
    logger.info(f"워크플로우 실행 시작: prompt_id={prompt_id}")

    output_videos = {}
    while True:
        out = ws.recv()
        if isinstance(out, str):
            message = json.loads(out)
            if message["type"] == "executing":
                data = message["data"]
                if data["node"] is not None:
                    logger.info(f"노드 실행 중: {data['node']}")
                if data["node"] is None and data["prompt_id"] == prompt_id:
                    logger.info("워크플로우 실행 완료")
                    break
        else:
            continue

    logger.info(f"히스토리 조회 중: prompt_id={prompt_id}")
    history = get_history(prompt_id)[prompt_id]
    logger.info(f"출력 노드 수: {len(history['outputs'])}")

    for node_id in history["outputs"]:
        node_output = history["outputs"][node_id]
        videos_output = []
        if "gifs" in node_output:
            logger.info(f"노드 {node_id}에서 {len(node_output['gifs'])}개의 비디오 발견")
            for idx, video in enumerate(node_output["gifs"]):
                video_path = video["fullpath"]
                logger.info(f"비디오 파일 경로: {video_path}")
                if os.path.exists(video_path):
                    file_size = os.path.getsize(video_path)
                    logger.info(f"비디오 {idx+1} 발견: {video_path} (크기: {file_size} bytes)")
                else:
                    logger.warning(f"비디오 파일이 존재하지 않습니다: {video_path}")
                videos_output.append(video_path)
        else:
            logger.info(f"노드 {node_id}에 비디오 출력 없음")
        output_videos[node_id] = videos_output

    logger.info(f"총 {len(output_videos)}개 노드에서 비디오 파일 경로 수집 완료")
    return output_videos


def load_workflow(workflow_path):
    with open(workflow_path, "r") as file:
        return json.load(file)


def get_workflow_path(input_type, person_count):
    if input_type == "image":
        if person_count == "single":
            return "/I2V_single.json"
        else:
            return "/I2V_multi.json"
    else:
        if person_count == "single":
            return "/V2V_single.json"
        else:
            return "/V2V_multi.json"


def get_audio_duration(audio_path):
    try:
        duration = librosa.get_duration(path=audio_path)
        return duration
    except Exception as e:
        logger.warning(f"오디오 길이 계산 실패 ({audio_path}): {e}")
        return None


def calculate_max_frames_from_audio(wav_path, wav_path_2=None, fps=25):
    durations = []
    duration1 = get_audio_duration(wav_path)
    if duration1 is not None:
        durations.append(duration1)
        logger.info(f"첫 번째 오디오 길이: {duration1:.2f}초")
    if wav_path_2:
        duration2 = get_audio_duration(wav_path_2)
        if duration2 is not None:
            durations.append(duration2)
            logger.info(f"두 번째 오디오 길이: {duration2:.2f}초")
    if not durations:
        logger.warning("오디오 길이를 계산할 수 없습니다. 기본값 81을 사용합니다.")
        return 81
    max_duration = max(durations)
    max_frames = int(max_duration * fps) + 81
    logger.info(f"가장 긴 오디오 길이: {max_duration:.2f}초, 계산된 max_frames: {max_frames}")
    return max_frames


def handler(job):
    job_input = job.get("input", {})

    log_input = job_input.copy()
    for key in ["image_base64", "video_base64", "wav_base64", "wav_base64_2"]:
        if key in log_input:
            log_input[key] = truncate_base64_for_log(log_input[key])

    logger.info(f"Received job input: {log_input}")
    task_id = f"task_{uuid.uuid4()}"

    input_type = job_input.get("input_type", "image")
    person_count = job_input.get("person_count", "single")

    logger.info(f"워크플로우 타입: {input_type}, 인물 수: {person_count}")

    workflow_path = get_workflow_path(input_type, person_count)
    logger.info(f"사용할 워크플로우: {workflow_path}")

    # 이미지/비디오 입력 처리
    media_path = None
    if input_type == "image":
        if "image_path" in job_input:
            media_path = process_input(job_input["image_path"], task_id, "input_image.jpg", "path")
        elif "image_url" in job_input:
            media_path = process_input(job_input["image_url"], task_id, "input_image.jpg", "url")
        elif "image_base64" in job_input:
            media_path = process_input(job_input["image_base64"], task_id, "input_image.jpg", "base64")
        else:
            media_path = "/examples/image.jpg"
            logger.info("기본 이미지 파일을 사용합니다: /examples/image.jpg")
    else:
        if "video_path" in job_input:
            media_path = process_input(job_input["video_path"], task_id, "input_video.mp4", "path")
        elif "video_url" in job_input:
            media_path = process_input(job_input["video_url"], task_id, "input_video.mp4", "url")
        elif "video_base64" in job_input:
            media_path = process_input(job_input["video_base64"], task_id, "input_video.mp4", "base64")
        else:
            media_path = "/examples/image.jpg"
            logger.info("기본 이미지 파일을 사용합니다: /examples/image.jpg")

    # 오디오 입력 처리
    wav_path = None
    wav_path_2 = None

    if "wav_path" in job_input:
        wav_path = process_input(job_input["wav_path"], task_id, "input_audio.wav", "path")
    elif "wav_url" in job_input:
        wav_path = process_input(job_input["wav_url"], task_id, "input_audio.wav", "url")
    elif "wav_base64" in job_input:
        wav_path = process_input(job_input["wav_base64"], task_id, "input_audio.wav", "base64")
    else:
        wav_path = "/examples/audio.mp3"
        logger.info("기본 오디오 파일을 사용합니다: /examples/audio.mp3")

    if person_count == "multi":
        if "wav_path_2" in job_input:
            wav_path_2 = process_input(job_input["wav_path_2"], task_id, "input_audio_2.wav", "path")
        elif "wav_url_2" in job_input:
            wav_path_2 = process_input(job_input["wav_url_2"], task_id, "input_audio_2.wav", "url")
        elif "wav_base64_2" in job_input:
            wav_path_2 = process_input(job_input["wav_base64_2"], task_id, "input_audio_2.wav", "base64")
        else:
            wav_path_2 = wav_path
            logger.info("두 번째 오디오가 없어 첫 번째 오디오를 사용합니다.")

    prompt_text = job_input.get("prompt", "A person talking naturally")
    width = job_input.get("width", 512)
    height = job_input.get("height", 512)

    max_frame = job_input.get("max_frame")
    if max_frame is None:
        logger.info("max_frame이 입력되지 않았습니다. 오디오 길이를 기반으로 자동 계산합니다.")
        max_frame = calculate_max_frames_from_audio(wav_path, wav_path_2 if person_count == "multi" else None)
    else:
        logger.info(f"사용자 지정 max_frame: {max_frame}")

    logger.info(f"워크플로우 설정: prompt='{prompt_text}', width={width}, height={height}, max_frame={max_frame}")
    logger.info(f"미디어 경로: {media_path}")
    logger.info(f"오디오 경로: {wav_path}")
    if person_count == "multi":
        logger.info(f"두 번째 오디오 경로: {wav_path_2}")

    prompt = load_workflow(workflow_path)

    # Force Offload 설정
    force_offload = job_input.get("force_offload", True)
    logger.info(f"🔧 설정: force_offload={force_offload}")

    sampler_node_id = None
    preferred_id = "128"
    if preferred_id in prompt and prompt[preferred_id].get("class_type") == "WanVideoSampler":
        sampler_node_id = preferred_id
    else:
        for node_id, node_data in prompt.items():
            if node_data.get("class_type") == "WanVideoSampler":
                sampler_node_id = node_id
                break

    if sampler_node_id:
        inputs = prompt[sampler_node_id].setdefault("inputs", {})
        inputs["force_offload"] = force_offload
        logger.info(f"✅ 노드 {sampler_node_id} (WanVideoSampler) 업데이트됨: force_offload={force_offload}")
    else:
        logger.warning("⚠️ 경고: WanVideoSampler 노드를 찾을 수 없습니다.")

    # 파일 존재 여부 확인
    if not os.path.exists(media_path):
        logger.error(f"미디어 파일이 존재하지 않습니다: {media_path}")
        return {"error": f"미디어 파일을 찾을 수 없습니다: {media_path}"}

    if not os.path.exists(wav_path):
        logger.error(f"오디오 파일이 존재하지 않습니다: {wav_path}")
        return {"error": f"오디오 파일을 찾을 수 없습니다: {wav_path}"}

    if person_count == "multi" and wav_path_2 and not os.path.exists(wav_path_2):
        logger.error(f"두 번째 오디오 파일이 존재하지 않습니다: {wav_path_2}")
        return {"error": f"두 번째 오디오 파일을 찾을 수 없습니다: {wav_path_2}"}

    logger.info(f"미디어 파일 크기: {os.path.getsize(media_path)} bytes")
    logger.info(f"오디오 파일 크기: {os.path.getsize(wav_path)} bytes")
    if person_count == "multi" and wav_path_2:
        logger.info(f"두 번째 오디오 파일 크기: {os.path.getsize(wav_path_2)} bytes")

    # 워크플로우 노드 설정
    if input_type == "image":
        prompt["284"]["inputs"]["image"] = media_path
    else:
        prompt["228"]["inputs"]["video"] = media_path

    prompt["125"]["inputs"]["audio"] = wav_path
    prompt["241"]["inputs"]["positive_prompt"] = prompt_text
    prompt["245"]["inputs"]["value"] = width
    prompt["246"]["inputs"]["value"] = height
    prompt["270"]["inputs"]["value"] = max_frame

    if person_count == "multi":
        if input_type == "image":
            if "307" in prompt:
                prompt["307"]["inputs"]["audio"] = wav_path_2
        else:
            if "313" in prompt:
                prompt["313"]["inputs"]["audio"] = wav_path_2

    ws_url = f"ws://{server_address}:8188/ws?clientId={client_id}"
    logger.info(f"Connecting to WebSocket: {ws_url}")

    http_url = f"http://{server_address}:8188/"
    logger.info(f"Checking HTTP connection to: {http_url}")

    max_http_attempts = 180
    for http_attempt in range(max_http_attempts):
        try:
            response = urllib.request.urlopen(http_url, timeout=5)
            logger.info(f"HTTP 연결 성공 (시도 {http_attempt+1})")
            break
        except Exception as e:
            logger.warning(f"HTTP 연결 실패 (시도 {http_attempt+1}/{max_http_attempts}): {e}")
            if http_attempt == max_http_attempts - 1:
                raise Exception("ComfyUI 서버에 연결할 수 없습니다.")
            import time
            time.sleep(1)

    ws = websocket.WebSocket()
    max_attempts = int(180 / 5)
    for attempt in range(max_attempts):
        import time
        try:
            ws.connect(ws_url)
            logger.info(f"웹소켓 연결 성공 (시도 {attempt+1})")
            break
        except Exception as e:
            logger.warning(f"웹소켓 연결 실패 (시도 {attempt+1}/{max_attempts}): {e}")
            if attempt == max_attempts - 1:
                raise Exception("웹소켓 연결 시간 초과 (3분)")
            time.sleep(5)

    videos = get_videos(ws, prompt, input_type, person_count)
    ws.close()
    logger.info("웹소켓 연결 종료")

    # FIX: 가장 큰 비디오 선택 (오디오 포함된 버전)
    output_video_path = None
    best_size = 0
    logger.info("출력 비디오 검색 중... (가장 큰 파일 선택)")

    for node_id in videos:
        if videos[node_id]:
            for vpath in videos[node_id]:
                if os.path.exists(vpath):
                    fsize = os.path.getsize(vpath)
                    logger.info(f"  후보: 노드 {node_id} → {vpath} ({fsize} bytes)")
                    if fsize > best_size:
                        best_size = fsize
                        output_video_path = vpath
                        logger.info(f"  ✅ 새로운 최대 파일: {vpath} ({fsize} bytes)")
                else:
                    logger.warning(f"  ⚠️ 파일 없음: {vpath}")
        else:
            logger.info(f"노드 {node_id}는 비어있음")

    if output_video_path:
        logger.info(f"🎬 최종 선택 비디오: {output_video_path} ({best_size} bytes)")

    if not output_video_path:
        logger.error("출력 비디오를 찾을 수 없습니다. 모든 노드가 비어있습니다.")
        return {"error": "비디오를 찾을 수 없습니다."}

    if not os.path.exists(output_video_path):
        logger.error(f"출력 비디오 파일이 존재하지 않습니다: {output_video_path}")
        return {"error": f"비디오 파일을 찾을 수 없습니다: {output_video_path}"}

    use_network_volume = job_input.get("network_volume", False)
    logger.info(f"네트워크 볼륨 사용 여부: {use_network_volume}")

    if use_network_volume:
        logger.info("네트워크 볼륨에 비디오 복사 시작")
        try:
            output_filename = f"infinitetalk_{task_id}.mp4"
            output_path = f"/runpod-volume/{output_filename}"
            logger.info(f"원본 파일: {output_video_path}")
            logger.info(f"대상 경로: {output_path}")

            source_file_size = os.path.getsize(output_video_path)
            logger.info(f"원본 파일 크기: {source_file_size} bytes")

            shutil.copy2(output_video_path, output_path)
            logger.info("파일 복사 완료")

            copied_file_size = os.path.getsize(output_path)
            logger.info(f"복사된 파일 크기: {copied_file_size} bytes")

            if source_file_size == copied_file_size:
                logger.info(f"✅ 결과 비디오를 '{output_path}'에 성공적으로 복사했습니다")
            else:
                logger.warning(f"⚠️ 파일 크기가 일치하지 않습니다: 원본={source_file_size}, 복사본={copied_file_size}")

            return {"video_path": output_path}

        except Exception as e:
            logger.error(f"❌ 비디오 복사 실패: {e}")
            return {"error": f"비디오 복사 실패: {e}"}
    else:
        logger.info("Base64 인코딩 시작")
        logger.info(f"비디오 파일 경로: {output_video_path}")

        try:
            file_size = os.path.getsize(output_video_path)
            logger.info(f"원본 파일 크기: {file_size} bytes")

            with open(output_video_path, "rb") as f:
                video_data = base64.b64encode(f.read()).decode("utf-8")

            encoded_size = len(video_data)
            logger.info(f"Base64 인코딩 완료: {encoded_size} 문자")
            logger.info(f"✅ Base64 인코딩된 비디오 반환: {truncate_base64_for_log(video_data)}")
            return {"video": video_data}

        except Exception as e:
            logger.error(f"❌ Base64 인코딩 실패: {e}")
            return {"error": f"Base64 인코딩 실패: {e}"}


runpod.serverless.start({"handler": handler})
