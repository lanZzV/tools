# -*- coding: UTF-8 -*-
# @author: 
# @file: async_slice_download
# @time: 2022-04-25
# @desc: 切片下载

import asyncio
import copy
import hashlib
import os
import shutil
from concurrent.futures import ThreadPoolExecutor

import aiofiles
import httpx
import requests
import urllib3
from loguru import logger

urllib3.disable_warnings()


def myhash(text):
    hash_lib = hashlib.md5()
    hash_lib.update(text.encode("utf-8"))
    return hash_lib.hexdigest()


class SliceDownloadBase:

    def __init__(self, url, method, headers=None, data=None, **request_kwargs):
        self.url = url
        self.method = method
        self.headers = headers or {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.72 Safari/537.36",
            "Referer": url
        }
        self.data = data
        self.is_proxy = True if request_kwargs.get("is_proxy") else False
        self.slice_size = request_kwargs.get("slice_size", 2 * 1024 * 1024)  # 自定义或默认分片大小为2M
        self.slice_min_size = request_kwargs.get("slice_min_size", 2 * 1024 * 1024)  # 小于slice_min_size的切片不再分片
        self.slice_semaphore = request_kwargs.get("slice_semaphore", 20)  # 自定义或默认分片并发数为10
        self.slice_cache = request_kwargs.get("slice_cache", False)  # 是否缓存分片
        self.slice_timeout = request_kwargs.get("slice_timeout") or 30  # 自定义或默认分片超时时间为30s
        self.slice_retry_times = request_kwargs.get("slice_retry_times", 10)  # 自定义或默认分片重试次数为10
        self.err_list_retry_times = request_kwargs.get("err_list_retry_times", 1)  # 自定义或默认失败列表重试次数为1
        self.unique_id = myhash(url)  # 根据url生成md5唯一id
        self.err_list = []  # 存储错误切片任务进行重试
        self.success_list = []  # 存储成功切片任务
        self.cache_dict = {}  # 存储加载的缓存切片
        self.rw_semaphore = None  # 读写文件并发数 异步信号量与 asyncio.run 连用时,需要在异步函数中设置

    # TODO 替换获取代理方法
    @staticmethod
    def get_proxy():
        # ip = get_proxy()
        # logger.debug(f"get proxy: {ip}")
        # return ip
        return ""

    def check_is_cached(self):
        """检查是否有unique_id对应的缓存文件夹"""
        if not os.path.exists("cache_down"):
            os.mkdir("cache_down")
        if not os.path.exists(f"cache_down/{self.unique_id}"):
            return False
        else:
            return True

    async def read_file(self, file_name):
        async with self.rw_semaphore:
            try:
                file_path = f"cache_down/{self.unique_id}/{file_name}"
                async with aiofiles.open(file_path, "rb") as f:
                    file_content = await f.read()
                index = int(file_name.split(".")[0])
                self.cache_dict[index] = file_content
            except:
                pass

    async def write_file(self, file_name, file_content):
        async with self.rw_semaphore:
            try:
                file_path = f"cache_down/{self.unique_id}/{file_name}.part"
                async with aiofiles.open(file_path, "wb") as f:
                    await f.write(file_content)
            except:
                pass

    async def load_cache(self):
        self.rw_semaphore = asyncio.Semaphore(10)  # 读写文件并发数为10
        if self.check_is_cached():
            files = os.listdir(f"cache_down/{self.unique_id}")
            cache_tasks = [asyncio.create_task(self.read_file(file_name)) for file_name in files]
            await asyncio.gather(*cache_tasks, return_exceptions=False)
            logger.success(f"【{self.url}】共加载成功{len(list(self.cache_dict.keys()))}个缓存切片!")
        # else:
        #     logger.info(f"【{self.url}】没有历史缓存文件需要加载!")

    async def save_cache(self):
        if not os.path.exists(f"cache_down/{self.unique_id}"):
            os.mkdir(f"cache_down/{self.unique_id}")
        save_cache_tasks = []
        for index, content in enumerate(self.success_list):
            if content:
                save_cache_tasks.append(asyncio.create_task(self.write_file(str(index), content)))
        await asyncio.gather(*save_cache_tasks, return_exceptions=False)
        logger.success(f"【{self.url}】共保存成功{len(save_cache_tasks)}个缓存切片!")

    def remove_cache_dir(self):
        if self.check_is_cached():
            shutil.rmtree(f"cache_down/{self.unique_id}")

    def calc_slice_task(self, file_size):
        slice_list = []
        index = 0
        end = -1
        while end < file_size - 1:
            start = end + 1
            end = start + self.slice_size - 1
            total_size = end - start + 1
            if end > file_size:
                end = file_size
                total_size = end - start
            headers = {'Range': 'bytes={0}-{1}'.format(start, end)}
            slice_list.append((index, total_size, headers))
            index += 1
        logger.info(f'【{self.url}】获取切片任务数:{len(slice_list)}')
        self.success_list = [b""] * len(slice_list)  # 按切片任务数初始化成功切片列表
        return slice_list

    def merge_slice(self):
        return b"".join(self.success_list)


class AsyncSliceDownload(SliceDownloadBase):

    def __init__(self, url, method, headers=None, data=None, **request_kwargs):
        super().__init__(url, method, headers, data, **request_kwargs)

    async def get_file_size(self):
        count = 0
        while count < 3:
            try:
                headers = copy.deepcopy(self.headers)
                headers["Range"] = "bytes=0-{}".format(100)
                proxies = None
                if self.is_proxy:
                    ip = self.get_proxy()
                    if ip:
                        proxies = {
                            "http://": "http://{}".format(ip),
                            "https://": "https://{}".format(ip)
                        }
                async with httpx.AsyncClient(proxies=proxies, trust_env=False, verify=False) as client:
                    response = await client.request(self.method, self.url, headers=headers, timeout=self.slice_timeout)
                if response.status_code == 206:
                    content_range = response.headers.get("Content-Range")
                    if content_range:
                        return int(content_range.split("/")[-1])
                elif response.status_code == 200:
                    return response.content
                else:
                    raise Exception(f"【{self.url}】请求状态码异常:{response.status_code}")
            except Exception as e:
                count += 1
                if count == 3:
                    logger.error(f"【{self.url}】获取文件大小重试{count}次失败,本次放弃")
                    return None
                logger.warning(f"【{self.url}】获取文件大小异常:{e},正在重试第{count}次")

    async def slice_download(self, slice_task, slice_semaphore, count=0):
        async with slice_semaphore:
            headers = copy.deepcopy(self.headers)
            index = slice_task[0]
            if self.slice_cache:
                if self.cache_dict.get(index):
                    self.success_list[index] = self.cache_dict.pop(index)
                    logger.info(f"【{self.url}】加载缓存{index}号切片成功!")
                    return
            verify_size = slice_task[1]
            if slice_task[2]["Range"]:
                headers["Range"] = slice_task[2]["Range"]
            try:
                proxies = None
                if self.is_proxy:
                    ip = self.get_proxy()
                    if ip:
                        proxies = {
                            "http://": "http://{}".format(ip),
                            "https://": "https://{}".format(ip)
                        }
                async with httpx.AsyncClient(proxies=proxies, trust_env=False, verify=False) as client:
                    response = await client.request(self.method, self.url, headers=headers, timeout=self.slice_timeout)
                if response.status_code == 206 or response.status_code == 200:
                    if len(response.content) != int(verify_size):
                        count += 1
                        if count < self.slice_retry_times:
                            logger.warning(
                                f"【{self.url}】{index}号切片文件大小校验失败:"
                                f"{[len(response.content), int(verify_size)], response.headers.get('Content-Length') or response.headers.get('content-length')}"
                                f",正在重试第{count}次")
                            await self.slice_download(slice_task, slice_semaphore, count)
                        else:
                            logger.error(f"【{self.url}】{index}号切片下载重试后失败,本次放弃")
                            self.err_list.append(slice_task)
                            return
                    else:
                        self.success_list[index] = response.content
                        # logger.debug(f"{index}号切片下载成功")
                        return
                else:
                    count += 1
                    logger.warning(f"【{self.url}】{index}号切片下载状态码异常:{response.status_code},正在重试第{count}次")
                    await self.slice_download(slice_task, slice_semaphore, count)
            except Exception as e:
                count += 1
                if count < self.slice_retry_times:
                    logger.warning(f"【{self.url}】{index}号切片下载异常:{e},正在重试第{count}次")
                    await self.slice_download(slice_task, slice_semaphore, count)
                else:
                    logger.error(f"【{self.url}】{index}号切片下载重试后失败,本次放弃")
                    self.err_list.append(slice_task)
                    return

    async def download(self):
        """
        异步下载
        :return: (state, content) state 0:切换普通方式 1:切片下载失败 2:下载成功
        """
        if self.slice_cache:
            await self.load_cache()
        file_size = await self.get_file_size()
        if not file_size:
            logger.warning(f"【{self.url}】获取file_size异常,转用普通下载方式")
            return 0, b""
        if isinstance(file_size, bytes):
            logger.debug(f"【{self.url}】附件不支持切片下载功能,直接下载成功!")
            return 2, file_size
        logger.debug(f"开始下载:【{self.url}】,文件大小:【{round(file_size / (1024 * 1024), 2)}mb】")
        slice_semaphore = asyncio.Semaphore(self.slice_semaphore)
        if file_size <= self.slice_min_size:
            logger.info(f"【{self.url}】文件小于切片最小值,直接下载")
            self.success_list = [b""]
            slice_task = [0, file_size, {"Range": None}]
            await self.slice_download(slice_task, slice_semaphore)
        else:
            slice_tasks = self.calc_slice_task(file_size)
            down_tasks = [asyncio.create_task(self.slice_download(task, slice_semaphore)) for task in slice_tasks]
            await asyncio.gather(*down_tasks, return_exceptions=False)
            for i in range(self.err_list_retry_times):
                if self.err_list:
                    logger.info(f"【{self.url}】本次有{len(self.err_list)}个切片下载失败,开始重试下载")
                    retry_slice_tasks = self.err_list[:]
                    self.err_list = []
                    retry_down_tasks = [asyncio.create_task(self.slice_download(task, slice_semaphore)) for task in
                                        retry_slice_tasks]
                    await asyncio.gather(*retry_down_tasks, return_exceptions=False)
            if self.err_list:
                logger.error(f"【{self.url}】重试下载后还有{len(self.err_list)}个切片下载失败,本次下载失败")
                if self.slice_cache:
                    await self.save_cache()
                return 1, b""
        file_content = self.merge_slice()
        if len(file_content) != file_size:
            logger.error(f"【{self.url}】下载后文件大小不等于文件大小,本次下载失败")
            self.remove_cache_dir()  # 删除缓存文件夹
            return 1, b""
        logger.success(f"【{self.url}】下载成功")
        if self.slice_cache:
            self.remove_cache_dir()  # 下载成功后删除缓存文件夹
        return 2, file_content


class ThreadSliceDownload(SliceDownloadBase):

    def __init__(self, url, method, headers=None, data=None, **request_kwargs):
        super().__init__(url, method, headers, data, **request_kwargs)

    def get_file_size(self):
        count = 0
        while count < 3:
            try:
                headers = copy.deepcopy(self.headers)
                headers["Range"] = "bytes=0-{}".format(100)
                proxies = None
                if self.is_proxy:
                    ip = self.get_proxy()
                    if ip:
                        proxies = {
                            "http": "{}".format(ip),
                            "https": "{}".format(ip)
                        }
                response = requests.request(self.method, self.url, headers=headers, proxies=proxies,
                                            timeout=self.slice_timeout, verify=False)
                if response.status_code == 206:
                    content_range = response.headers.get("Content-Range")
                    if content_range:
                        return int(content_range.split("/")[-1])
                elif response.status_code == 200:
                    return response.content
                else:
                    raise Exception(f"【{self.url}】请求状态码异常:{response.status_code}")
            except Exception as e:
                count += 1
                if count == 3:
                    logger.error(f"【{self.url}】获取文件大小重试{count}次失败,本次放弃")
                    return None
                logger.warning(f"【{self.url}】获取文件大小异常:{e},正在重试第{count}次")

    def slice_download(self, slice_task, count=0):
        headers = copy.deepcopy(self.headers)
        index = slice_task[0]
        if self.slice_cache:
            if self.cache_dict.get(index):
                self.success_list[index] = self.cache_dict.pop(index)
                logger.info(f"【{self.url}】加载缓存{index}号切片成功!")
                return
        verify_size = slice_task[1]
        if slice_task[2]["Range"]:
            headers["Range"] = slice_task[2]["Range"]
        try:
            proxies = None
            if self.is_proxy:
                ip = self.get_proxy()
                if ip:
                    proxies = {
                        "http": "{}".format(ip),
                        "https": "{}".format(ip)
                    }
            response = requests.request(self.method, self.url, headers=headers, proxies=proxies,
                                        timeout=self.slice_timeout, verify=False)
            if response.status_code == 206 or response.status_code == 200:
                if len(response.content) != int(verify_size):
                    count += 1
                    if count < self.slice_retry_times:
                        logger.warning(
                            f"【{self.url}】{index}号切片文件大小校验失败:"
                            f"{[len(response.content), int(verify_size)], response.headers.get('Content-Length') or response.headers.get('content-length')}"
                            f",正在重试第{count}次")
                        self.slice_download(slice_task, count)
                    else:
                        logger.error(f"【{self.url}】{index}号切片下载重试后失败,本次放弃")
                        self.err_list.append(slice_task)
                        return
                else:
                    # self.success_list[index] = response.content
                    # logger.debug(f"{index}号切片下载成功")
                    return
            else:
                count += 1
                logger.warning(f"【{self.url}】{index}号切片下载状态码异常:{response.status_code},正在重试第{count}次")
                self.slice_download(slice_task, count)
        except Exception as e:
            count += 1
            if count < self.slice_retry_times:
                logger.warning(f"【{self.url}】{index}号切片下载异常:{e},正在重试第{count}次")
                self.slice_download(slice_task, count)
            else:
                logger.error(f"【{self.url}】{index}号切片下载重试后失败,本次放弃")
                self.err_list.append(slice_task)
                return

    async def download(self):
        """
        多线程下载
        :return: (state, content) state 0:切换普通方式 1:切片下载失败 2:下载成功
        """
        if self.slice_cache:
            await self.load_cache()
        file_size = self.get_file_size()
        if not file_size:
            logger.warning(f"【{self.url}】获取file_size异常,转用普通下载方式")
            return 0, b""
        if isinstance(file_size, bytes):
            logger.debug(f"【{self.url}】附件不支持切片下载功能,直接下载成功!")
            return 2, file_size
        logger.debug(f"开始下载:【{self.url}】,文件大小:【{round(file_size / (1024 * 1024), 2)}mb】")
        if file_size <= self.slice_min_size:
            logger.info(f"【{self.url}】文件小于切片最小值,直接下载")
            self.success_list = [b""]
            slice_task = [0, file_size, {"Range": None}]
            with ThreadPoolExecutor(self.slice_semaphore) as executor:
                executor.map(self.slice_download, [slice_task])
        else:
            slice_tasks = self.calc_slice_task(file_size)
            with ThreadPoolExecutor(self.slice_semaphore) as executor:
                executor.map(self.slice_download, slice_tasks)
            for i in range(self.err_list_retry_times):
                if self.err_list:
                    logger.info(f"【{self.url}】本次有{len(self.err_list)}个切片下载失败,开始重试下载")
                    retry_slice_tasks = self.err_list[:]
                    self.err_list = []
                    with ThreadPoolExecutor(self.slice_semaphore) as executor:
                        executor.map(self.slice_download, retry_slice_tasks)
            if self.err_list:
                logger.error(f"【{self.url}】重试下载后还有{len(self.err_list)}个切片下载失败,本次下载失败")
                if self.slice_cache:
                    await self.save_cache()
                return 1, b""
        file_content = self.merge_slice()
        if len(file_content) != file_size:
            logger.error(f"【{self.url}】【{self.unique_id}】下载后文件大小不等于文件大小,本次下载失败")
            self.remove_cache_dir()  # 删除缓存文件夹
            return 1, b""
        logger.success(f"【{self.url}】下载成功")
        if self.slice_cache:
            self.remove_cache_dir()  # 下载成功后删除缓存文件夹
        return 2, file_content


if __name__ == '__main__':
    t_url = "xxx"
    t_method = "GET"
    t_slice_config = {
        "slice_size": 2 * 1024 * 1024,  # 切片大小
        "slice_min_size": 2 * 1024 * 1024,  # 切片最小大小(小于该大小不进行切片,但会进行文件大小校验)
        "slice_semaphore": 50,  # 切片最大并发量
        "slice_timeout": 20,  # 切片超时时间
        "slice_cache": True,  # 切片缓存功能
        "slice_mode": "thread",  # 切片模式，thread：多线程 默认异步
        "is_proxy": False,  # 是否使用代理
        "slice_retry_times": 10,  # 切片重试次数为10(单个切片重试次数)
        "err_list_retry_times": 1  # 失败列表重试次数为1(完整任务执行完后 失败列表整体重试次数)
    }
    if t_slice_config["slice_mode"] == "thread":
        logger.info("切片模式:多线程")
        state, file_data = asyncio.run(ThreadSliceDownload(t_url, t_method, **t_slice_config).download())
    else:
        logger.info("切片模式:异步")
        state, file_data = asyncio.run(AsyncSliceDownload(t_url, t_method, **t_slice_config).download())
    logger.debug(f"下载完成:state: {state}, file_size: {round(len(file_data) / (1024 * 1024), 2)}mb")
