"""请求 / 响应模型。"""

from __future__ import annotations

import base64

from pydantic import BaseModel, Field


class WriteRequest(BaseModel):
    """定长数据写入。

    ``data`` 为原始字符串（按 UTF-8 编码，字节数须恰好等于 BLOCK_SIZE）；
    若内容含二进制或不可打印字符，可改用 ``data_base64``。两者只能给一个。
    """

    data: str | None = Field(default=None, description="原始字符串，UTF-8 编码后长度须等于 BLOCK_SIZE")
    data_base64: str | None = Field(default=None, description="base64 编码的定长数据，解码后长度须等于 BLOCK_SIZE")

    def decode(self, block_size: int) -> bytes:
        if (self.data is None) == (self.data_base64 is None):
            raise ValueError("data 与 data_base64 必须且只能提供一个")
        if self.data is not None:
            return self.data.encode("utf-8")
        try:
            return base64.b64decode(self.data_base64, validate=True)
        except Exception as exc:  # pragma: no cover - 透传给 400
            raise ValueError("data_base64 不是合法 base64") from exc


class BlockResponse(BaseModel):
    group_id: int
    block_id: int
    seq: int
    version: int = Field(
        description="写入版本：每次写入全局单调递增；搬迁拷贝沿用原版本"
    )
    region_id: int
    slot_index: int
    length: int
    data_ascii: str = Field(description="可打印部分的 ASCII 表示，不可打印字节以 . 代替")
    data_base64: str


class RegionResponse(BaseModel):
    region_id: int
    status: str
    group_id: int | None
    used_slots: int
    total_slots: int
    free_slots: int
    valid_blocks: int


class SlotRecord(BaseModel):
    seq: int
    slot_index: int
    group_id: int
    block_id: int
    version: int
    data_base64: str


class DiscardItem(BaseModel):
    """单个丢弃项：块号 + 客户端从读口拿到的写入版本。"""

    block_id: int = Field(ge=0, description="逻辑块号")
    version: int = Field(ge=1, description="该块当前的写入版本（读口返回的 version）")


class DiscardRequest(BaseModel):
    """单组批量丢弃请求；块号必须互不重复，整批原子生效或整批 409。"""

    blocks: list[DiscardItem] = Field(
        min_length=1, description="待丢弃块清单，块号互不重复"
    )


class DiscardResponse(BaseModel):
    status: str
    group_id: int
    discarded: list[DiscardItem]


class RegionDetailResponse(RegionResponse):
    records: list[SlotRecord]


class MigrationResponse(BaseModel):
    in_progress: bool
    src_region_id: int | None = None
    dst_region_id: int | None = None
    group_id: int | None = None
    pending_block_id: int | None = None
    pending_applied: bool | None = None
    last_copied_block_id: int | None = None
    dst_used_slots: int | None = None
    src_distinct_blocks: int | None = None
    last_step: dict | None = None


class ConfigResponse(BaseModel):
    regions: int
    groups: int
    slots_per_region: int
    block_size: int
