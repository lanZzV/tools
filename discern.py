# -*- coding: UTF-8 -*-
# @author: zhulw
# @file: localdiscern
# @time: 2022-06-17
# @desc:
import os
import shutil
from typing import List

import cv2
import numpy as np
from loguru import logger


class SlideCrack:

    def __init__(self, front, bg, out=None):
        self.front = front
        self.bg = bg
        self.out = out

    @staticmethod
    def clear_white(img):
        img = cv2.imdecode((np.frombuffer(img, np.uint8)), cv2.IMREAD_COLOR)
        rows, cols, channel = img.shape
        min_x = 255
        min_y = 255
        max_x = 0
        max_y = 0
        for x in range(1, rows):
            for y in range(1, cols):
                t = set(img[x, y])
                if len(t) >= 2:
                    if x <= min_x:
                        min_x = x
                    elif x >= max_x:
                        max_x = x
                    if y <= min_y:
                        min_y = y
                    elif y >= max_y:
                        max_y = y
        img1 = img[min_x:max_x, min_y: max_y]
        return img1

    def template_match(self, tpl, target):
        th, tw = tpl.shape[:2]
        result = cv2.matchTemplate(target, tpl, cv2.TM_CCOEFF_NORMED)
        min_val, max_val, min_loc, max_loc = cv2.minMaxLoc(result)
        tl = max_loc
        br = (tl[0] + tw, tl[1] + th)
        cv2.rectangle(target, tl, br, (0, 0, 255), 2)
        if self.out:
            cv2.imwrite(self.out, target)
        return tl[0]

    @staticmethod
    def image_edge_detection(img):
        edges = cv2.Canny(img, 100, 200)
        return edges

    def discern(self):
        img1 = self.clear_white(self.front)
        img1 = cv2.cvtColor(img1, cv2.COLOR_RGB2GRAY)
        slide = self.image_edge_detection(img1)

        back = cv2.imdecode((np.frombuffer(self.bg, np.uint8)), cv2.COLOR_RGB2GRAY)
        back = self.image_edge_detection(back)

        slide_pic = cv2.cvtColor(slide, cv2.COLOR_GRAY2RGB)
        back_pic = cv2.cvtColor(back, cv2.COLOR_GRAY2RGB)
        x = self.template_match(slide_pic, back_pic)
        return int(x)


class ClickCrack:
    """显示图片, 双击返回坐标, ESC 退出并返回结果"""

    def __init__(self, bg: bytes, small_imgs: bytes or List[bytes] = None):
        """

        :param bg: 底图
        :param small_imgs: 小图 (列表) 用于显示点击顺序, 可不传
        """
        self.bg = bg
        self.small_imgs = [small_imgs] if small_imgs and isinstance(small_imgs, bytes) else small_imgs
        self.points = []

    def add_point(self, event, x, y, *args, **kwargs):
        if event == cv2.EVENT_LBUTTONDBLCLK:
            self.points.append((x, y))
            logger.debug(f"当前点击: {(x, y)}")

    def discern(self):
        if self.small_imgs:
            for i, img in enumerate(self.small_imgs):
                if not os.path.exists("tmp"):
                    os.mkdir("tmp")
                cv2.imwrite(f"tmp/{i}.png", cv2.imdecode((np.frombuffer(img, np.uint8)), cv2.IMREAD_UNCHANGED))
            logger.info("需要点击的小图顺序已写入 tmp 文件夹")
        logger.info(f"开始点击, 双击确定坐标, 点击完毕后按 ESC 退出")
        cv2.namedWindow("image", cv2.WINDOW_NORMAL)
        cv2.setMouseCallback("image", self.add_point)
        cv2.imshow("image", cv2.imdecode((np.frombuffer(self.bg, np.uint8)), cv2.IMREAD_UNCHANGED))
        while True:
            if cv2.waitKey(20) & 0xFF == 27:  # 按ESC退出
                break
        cv2.destroyAllWindows()
        if os.path.exists("tmp"):
            shutil.rmtree("tmp")
        return self.points
