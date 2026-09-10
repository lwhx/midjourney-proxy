using Serilog;
using Midjourney.Base.Data;

namespace Midjourney.Base
{
    /// <summary>
    /// 悠船强制优先慢速
    /// 开启「优先消耗慢速」的账号慢速总剩余大于阈值时，快速/极速请求优先以慢速选中这些账号
    /// 读路径无锁不阻塞；刷新采用 CAS 抢占，同一时刻最多一条线程在计算，其余线程直接使用旧值
    /// </summary>
    public static class YouChuanRelaxPool
    {
        /// <summary>
        /// 缓存时长（毫秒），避免每次选号都全池计算
        /// </summary>
        private const long CacheMilliseconds = 60_000;

        /// <summary>
        /// 慢速总剩余缓存值
        /// </summary>
        private static int _remainingTotal;

        /// <summary>
        /// 上次刷新时间（Environment.TickCount64）
        /// </summary>
        private static long _lastRefreshTick;

        /// <summary>
        /// 刷新抢占标记：0 = 空闲，1 = 刷新中
        /// </summary>
        private static int _refreshing;

        /// <summary>
        /// 是否命中「强制优先慢速」：开关开启 且 开启「优先消耗慢速」的账号慢速总剩余 > 阈值
        /// </summary>
        public static bool IsForcePreferRelaxActive()
        {
            var setting = GlobalConfiguration.Setting;
            if (setting.YouChuanForcePreferRelax != true)
            {
                return false;
            }

            return GetRemainingTotal() > setting.YouChuanForcePreferRelaxThreshold;
        }

        /// <summary>
        /// 开启「优先消耗慢速」的账号慢速总剩余次数（60 秒内存快照，一次 DB 查询 + 一次 Redis HGETALL）
        /// 读取永远不等待：缓存命中直接返回；缓存过期时仅一条线程刷新，其余线程沿用旧值
        /// 刷新失败（DB/Redis 异常）时沿用上次缓存值并推进刷新时间，60 秒内不会反复重试
        /// </summary>
        public static int GetRemainingTotal()
        {
            // 快路径：无锁读缓存
            if (Environment.TickCount64 - Volatile.Read(ref _lastRefreshTick) < CacheMilliseconds)
            {
                return Volatile.Read(ref _remainingTotal);
            }

            // CAS 抢占刷新权，抢占失败的线程直接用旧值，绝不等待
            if (Interlocked.CompareExchange(ref _refreshing, 1, 0) != 0)
            {
                return Volatile.Read(ref _remainingTotal);
            }

            try
            {
                var total = ComputeRemainingTotal();

                Volatile.Write(ref _remainingTotal, total);

                Log.Information("慢速总剩余 {@0}", total);

                return total;
            }
            catch (Exception ex)
            {
                Log.Warning(ex, "计算悠船慢速总剩余异常，沿用上次缓存值");

                // 异常时沿用上次缓存值，首次失败返回 0（即不启用强制优先慢速，走原有分配）
                return Volatile.Read(ref _remainingTotal);
            }
            finally
            {
                // 无论成功失败都推进刷新时间：故障期间最多每 60 秒重试一次，不会反复触发计算
                Volatile.Write(ref _lastRefreshTick, Environment.TickCount64);
                Volatile.Write(ref _refreshing, 0);
            }
        }

        /// <summary>
        /// 计算开启「优先消耗慢速」的账号慢速总剩余次数
        /// </summary>
        private static int ComputeRemainingTotal()
        {
            var today = DateTime.Now.Date;

            var accounts = FreeSqlHelper.FreeSql.Select<DiscordAccount>()
                .Where(c => c.Enable == true && c.IsYouChuan && c.YouChuanEnablePreferRelax)
                .ToList();

            // 一次批量取今日慢速计数，避免逐号查询
            var usedDict = CounterHelper.GetAllYouchuanRelaxCountDict();

            var total = 0;
            foreach (var account in accounts)
            {
                // 慢速已超量锁定
                if (account.YouChuanRelaxedReset > today)
                {
                    continue;
                }

                if (account.YouChuanRelaxDailyLimit <= 0)
                {
                    continue;
                }

                var cap = account.YouChuanRelaxDailyLimit;
                if (account.DayRelaxDrawLimit > 0)
                {
                    cap = Math.Min(cap, account.DayRelaxDrawLimit);
                }

                usedDict.TryGetValue(account.ChannelId, out var used);

                var remaining = cap - used;
                if (remaining > 0)
                {
                    total += remaining;
                }
            }

            return total;
        }
    }
}
