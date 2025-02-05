package catabalancer

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/load"
	"github.com/shirou/gopsutil/v3/mem"
)

type LoadAverage struct {
	Load1Min  float64
	Load5Min  float64
	Load15Min float64
}

type SystemUsage struct {
	CPUUsagePercentage float64
	RAMUsagePercentage float64
	BWUsagePercentage  float64
	LoadAvg            LoadAverage
}

type SystemInfo struct {
	CPUInfo  []cpu.InfoStat
	MemInfo  *mem.VirtualMemoryStat
	DiskInfo []disk.UsageStat
}

type BandwidthData struct {
	JSONVersion   string    `json:"jsonversion"`
	VnstatVersion string    `json:"vnstatversion"`
	Interface     string    `json:"interface"`
	SampleTime    int       `json:"sampletime"`
	Rx            Bandwidth `json:"rx"`
	Tx            Bandwidth `json:"tx"`
}

type Bandwidth struct {
	RateString       string `json:"ratestring"`
	BytesPerSecond   int    `json:"bytespersecond"`
	PacketsPerSecond int    `json:"packetspersecond"`
	Bytes            int    `json:"bytes"`
	Packets          int    `json:"packets"`
}

func GetSystemUsage() (SystemUsage, error) {
	var systemUsage SystemUsage

	// Get CPU usage
	cpuPercents, err := cpu.Percent(time.Second, false)
	if err != nil {
		return systemUsage, err
	}
	if len(cpuPercents) > 0 {
		systemUsage.CPUUsagePercentage = cpuPercents[0]
	} else {
		return SystemUsage{}, fmt.Errorf("cpu usage not found")
	}

	// Get memory usage
	vmStat, err := mem.VirtualMemory()
	if err != nil {
		return systemUsage, err
	}
	systemUsage.RAMUsagePercentage = vmStat.UsedPercent

	// Get BW usage
	bw, _ := GetBandwidthUsage() // ignore errors for now until we have vnstat rolled out everywhere
	systemUsage.BWUsagePercentage = bw

	// Get Load Average
	avg, err := load.Avg()
	if err != nil {
		return systemUsage, err
	}
	systemUsage.LoadAvg = LoadAverage{
		Load1Min:  avg.Load1,
		Load5Min:  avg.Load5,
		Load15Min: avg.Load15,
	}

	return systemUsage, nil
}

// Get bandwidth usage using the vnstat utility.
// 'vnstat --json --iface en0 -tr 2.5' calculates traffic for given interface
// over the specified duration in seconds
func GetBandwidthUsage() (float64, error) {
	iface := "eth0"
	trafficArgs := []string{"-tr", "2.5"}
	args := append([]string{"--json", "--iface", iface}, trafficArgs...)

	// Run vnstat
	cmd := exec.Command("vnstat", args...)
	cmd.Env = os.Environ()
	output, err := cmd.CombinedOutput()
	if err != nil {
		return -1, err
	}

	// Parse json output
	var data BandwidthData
	err = json.Unmarshal(output, &data)
	if err != nil {
		return -1, err
	}

	// Get bandwidth in Mbits/s
	totalBW := (float64(data.Rx.BytesPerSecond + data.Tx.BytesPerSecond)) / 8 / 1e6

	// Get the NIC's speed
	nicSpeedFilePath := fmt.Sprintf("/sys/class/net/%s/speed", iface)
	speedContent, err := os.ReadFile(nicSpeedFilePath)
	if err != nil {
		return -1, err
	}
	speedStr := strings.TrimSpace(string(speedContent))
	speed, err := strconv.ParseFloat(speedStr, 64)
	if err != nil {
		return -1, err
	}

	// Calculate bandwidth usage
	totalBWPercent := (totalBW / speed) * 100
	return totalBWPercent, nil
}
