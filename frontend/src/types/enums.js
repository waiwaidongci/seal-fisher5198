export const ROLES=['admin','analyst','viewer'];
export const STATES=['queued','running','completed','failed'];
export const STATE_LABELS={queued:'等待接取',running:'计算进行中',completed:'已完成',failed:'失败需修复'};
// Stage copy used when a run is waiting, retrying or under integrity review.
export const STAGE_NOTES={
 queued:'排队登记已保存，正在等待空闲计算进程接取；通知可重发，编号不变。',
 running:'计算进程持有租约并持续发送心跳；若中断，过期后由其他进程自动接管。',
 failed:'本次无结果。可沿用原编号重试，已保存结果不会重算。',
 integrity:'数据库结果与留档校验未通过，查看、复核与下载已关闭，等待原件恢复。'
};
