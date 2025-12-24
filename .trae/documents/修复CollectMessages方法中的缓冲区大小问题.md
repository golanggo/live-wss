1. 修改CollectMessages方法，使用动态获取的缓冲区大小bufSize替代硬编码的ringBufferSize
2. 确保索引计算使用正确的缓冲区大小，避免数组越界
3. 移除对未定义常量ringBufferSize的依赖
4. 确保代码与动态调整缓冲区大小的逻辑保持一致

