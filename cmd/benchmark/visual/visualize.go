package visual

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/go-echarts/go-echarts/v2/charts"
	"github.com/go-echarts/go-echarts/v2/components"
	"github.com/go-echarts/go-echarts/v2/opts"
	"github.com/go-echarts/go-echarts/v2/types"

	"raftkv/cmd/benchmark/load"
)

// 性能报告可视化器
type Visualizer struct {
	resultDir string
	testName  string
}

// 创建新的可视化器
func NewVisualizer(resultDir, testName string) *Visualizer {
	if testName == "" {
		testName = fmt.Sprintf("benchmark-%s", time.Now().Format("01-02_15-04-05"))
	}

	// 确保结果目录存在
	if resultDir == "" {
		resultDir = "res"
	}

	fullPath := filepath.Join(resultDir, testName)
	err := os.MkdirAll(fullPath, 0755)
	if err != nil {
		log.Printf("创建可视化结果目录失败: %v, 将使用当前目录", err)
		fullPath = testName
		_ = os.MkdirAll(fullPath, 0755)
	}

	return &Visualizer{
		resultDir: fullPath,
		testName:  testName,
	}
}

// 生成性能图表
func (v *Visualizer) GenerateCharts(stats []load.Stats, intervals []time.Time, params map[string]string) error {
	if len(stats) == 0 {
		return fmt.Errorf("没有性能数据可以可视化")
	}

	// 准备时间轴数据
	timeData := make([]string, len(intervals))
	for i, t := range intervals {
		timeData[i] = t.Format("15:04:05")
	}

	// 创建页面
	page := components.NewPage()
	page.SetLayout(components.PageFlexLayout)
	page.PageTitle = "RaftKV性能测试报告"

	// 生成各种图表并添加到页面
	qpsChart := v.createQPSChart(stats, timeData)
	latencyChart := v.createLatencyChart(stats, timeData)
	opTypePie := v.createOpTypePieChart(stats[len(stats)-1])
	errorRateChart := v.createErrorRateChart(stats, timeData)

	// 将所有图表添加到页面
	page.AddCharts(
		qpsChart,
		latencyChart,
		opTypePie,
		errorRateChart,
	)

	// 创建HTML文件并渲染所有图表
	f, err := os.Create(filepath.Join(v.resultDir, "performance_charts.html"))
	if err != nil {
		log.Printf("创建性能图表文件失败: %v", err)
		return err
	}
	defer f.Close()

	err = page.Render(f)
	if err != nil {
		log.Printf("渲染性能图表失败: %v", err)
		return err
	}

	// 生成摘要页面
	v.generateSummaryPage(stats[len(stats)-1], params)

	log.Println(createClickableLink("\n\n性能可视化报告已生成\n", v.resultDir+"/index.html"))
	return nil
}

func createClickableLink(text, path string) string {
	absPath, err := filepath.Abs(path)
	if err != nil {
		absPath = path
	}

	fileURL := "file://" + absPath

	return fmt.Sprintf("  \033]8;;%s\007%s\033]8;;\007", fileURL, text)
}

// 创建QPS图表
func (v *Visualizer) createQPSChart(stats []load.Stats, timeData []string) *charts.Line {
	// 创建折线图实例
	line := charts.NewLine()

	// 设置图表全局选项
	line.SetGlobalOptions(
		charts.WithInitializationOpts(opts.Initialization{
			Theme:  types.ThemeWesteros,
			Width:  "900px",
			Height: "700px",
		}),
		charts.WithTitleOpts(opts.Title{
			Title:    "每秒查询数 (QPS) 变化趋势",
			Subtitle: "随时间变化的QPS",
			Top:      "10%",
			Left:     "center",
			TitleStyle: &opts.TextStyle{
				FontFamily: "Source Han Sans CN",
			},
			SubtitleStyle: &opts.TextStyle{
				FontFamily: "Source Han Sans CN",
			},
		}),
		charts.WithLegendOpts(opts.Legend{
			Show: opts.Bool(true),
			Top:  "15%",
			Left: "right",
		}),
		charts.WithGridOpts(opts.Grid{
			Top:    "20%",
			Right:  "10%",
			Left:   "10%",
			Bottom: "20%",
		}),
		charts.WithTooltipOpts(opts.Tooltip{
			Show:    opts.Bool(true),
			Trigger: "axis",
		}),
		charts.WithYAxisOpts(opts.YAxis{
			Name:      "QPS",
			AxisLabel: &opts.AxisLabel{Show: opts.Bool(true)},
		}),
		charts.WithXAxisOpts(opts.XAxis{
			Name: "时间",
		}),
	)

	// 添加X轴数据
	line.SetXAxis(timeData)

	// 准备QPS数据
	qpsData := make([]opts.LineData, len(stats))
	for i, stat := range stats {
		qpsData[i] = opts.LineData{Value: stat.CurrentQPS}
	}

	// 添加QPS数据系列
	line.AddSeries("QPS", qpsData).
		SetSeriesOptions(
			charts.WithMarkPointNameTypeItemOpts(
				opts.MarkPointNameTypeItem{Name: "最大值", Type: "max"},
				opts.MarkPointNameTypeItem{Name: "最小值", Type: "min"},
			),
			charts.WithMarkLineNameTypeItemOpts(
				opts.MarkLineNameTypeItem{Name: "平均值", Type: "average"},
			),
		)

	return line
}

// 创建延迟图表
func (v *Visualizer) createLatencyChart(stats []load.Stats, timeData []string) *charts.Line {
	// 创建折线图实例
	line := charts.NewLine()

	// 设置图表全局选项
	line.SetGlobalOptions(
		charts.WithInitializationOpts(opts.Initialization{
			Theme:  types.ThemeWesteros,
			Width:  "900px",
			Height: "700px",
		}),
		charts.WithTitleOpts(opts.Title{
			Title:    "请求延迟趋势",
			Subtitle: "随时间变化的请求延迟（毫秒）",
			Top:      "8%",
			Left:     "center",
			TitleStyle: &opts.TextStyle{
				FontFamily: "Source Han Sans CN",
			},
			SubtitleStyle: &opts.TextStyle{
				FontFamily: "Source Han Sans CN",
			},
		}),
		charts.WithLegendOpts(opts.Legend{
			Show: opts.Bool(true),
			Top:  "15%",
			Left: "right",
		}),
		charts.WithGridOpts(opts.Grid{
			Top:    "20%",
			Right:  "10%",
			Left:   "10%",
			Bottom: "20%",
		}),
		charts.WithTooltipOpts(opts.Tooltip{
			Show:    opts.Bool(true),
			Trigger: "axis",
		}),
		charts.WithYAxisOpts(opts.YAxis{
			Name:      "延迟 (ms)",
			AxisLabel: &opts.AxisLabel{Show: opts.Bool(true)},
		}),
		charts.WithXAxisOpts(opts.XAxis{
			Name: "时间",
		}),
	)

	// 添加X轴数据
	line.SetXAxis(timeData)

	// 准备延迟数据
	avgLatencyData := make([]opts.LineData, len(stats))
	p50LatencyData := make([]opts.LineData, len(stats))
	p90LatencyData := make([]opts.LineData, len(stats))
	p99LatencyData := make([]opts.LineData, len(stats))

	for i, stat := range stats {
		avgLatencyData[i] = opts.LineData{Value: stat.AvgLatency}
		p50LatencyData[i] = opts.LineData{Value: stat.P50Latency}
		p90LatencyData[i] = opts.LineData{Value: stat.P90Latency}
		p99LatencyData[i] = opts.LineData{Value: stat.P99Latency}
	}

	// 添加延迟数据系列
	line.AddSeries("平均延迟", avgLatencyData).
		SetSeriesOptions(
			charts.WithLineChartOpts(opts.LineChart{
				Smooth: opts.Bool(true),
			}),
		)

	line.AddSeries("P50延迟", p50LatencyData).
		SetSeriesOptions(
			charts.WithLineChartOpts(opts.LineChart{
				Smooth: opts.Bool(true),
			}),
		)

	line.AddSeries("P90延迟", p90LatencyData).
		SetSeriesOptions(
			charts.WithLineChartOpts(opts.LineChart{
				Smooth: opts.Bool(true),
			}),
		)

	line.AddSeries("P99延迟", p99LatencyData).
		SetSeriesOptions(
			charts.WithLineChartOpts(opts.LineChart{
				Smooth: opts.Bool(true),
			}),
		)

	return line
}

// 创建操作类型饼图
func (v *Visualizer) createOpTypePieChart(finalStats load.Stats) *charts.Pie {
	// 创建饼图显示最终的操作分布
	pie := charts.NewPie()
	pie.SetGlobalOptions(
		charts.WithInitializationOpts(opts.Initialization{
			Theme:  types.ThemeWesteros,
			Width:  "900px",
			Height: "800px",
		}),
		charts.WithTitleOpts(opts.Title{
			Title:    "操作类型最终分布",
			Subtitle: "整体测试过程中的操作类型占比",
			Top:      "5%",
			Left:     "center",
			TitleStyle: &opts.TextStyle{
				FontFamily: "Source Han Sans CN",
			},
			SubtitleStyle: &opts.TextStyle{
				FontFamily: "Source Han Sans CN",
			},
		}),
		charts.WithLegendOpts(opts.Legend{
			Show: opts.Bool(true),
			Top:  "10%",
			Left: "right",
		}),
		charts.WithTooltipOpts(opts.Tooltip{
			Show:    opts.Bool(true),
			Trigger: "item",
		}),
	)

	// 添加饼图数据
	pie.AddSeries("操作类型", []opts.PieData{
		{Name: "读取操作", Value: finalStats.ReadOpsPercent},
		{Name: "写入操作", Value: finalStats.WriteOpsPercent},
		{Name: "追加操作", Value: finalStats.AppendOpsPercent},
	}).SetSeriesOptions(
		charts.WithLabelOpts(opts.Label{
			Show:      opts.Bool(true),
			Formatter: "{b}: {c}%",
		}),
		charts.WithPieChartOpts(opts.PieChart{
			Radius: []string{"40%", "70%"},
		}),
	)

	return pie
}

// 创建错误率图表
func (v *Visualizer) createErrorRateChart(stats []load.Stats, timeData []string) *charts.Line {
	// 创建折线图实例
	line := charts.NewLine()

	// 设置图表全局选项
	line.SetGlobalOptions(
		charts.WithInitializationOpts(opts.Initialization{
			Theme:  types.ThemeWesteros,
			Width:  "900px",
			Height: "800px",
		}),
		charts.WithTitleOpts(opts.Title{
			Title:    "错误率趋势",
			Subtitle: "随时间变化的请求错误率",
			Top:      "10%",
			Left:     "center",
			TitleStyle: &opts.TextStyle{
				FontFamily: "Source Han Sans CN",
			},
			SubtitleStyle: &opts.TextStyle{
				FontFamily: "Source Han Sans CN",
			},
		}),
		charts.WithLegendOpts(opts.Legend{
			Show: opts.Bool(true),
			Top:  "15%",
			Left: "right",
		}),
		charts.WithGridOpts(opts.Grid{
			Top:    "20%",
			Right:  "10%",
			Left:   "10%",
			Bottom: "30%",
		}),
		charts.WithTooltipOpts(opts.Tooltip{
			Show:    opts.Bool(true),
			Trigger: "axis",
		}),
		charts.WithYAxisOpts(opts.YAxis{
			Name:      "错误率 (%)",
			AxisLabel: &opts.AxisLabel{Show: opts.Bool(true)},
		}),
		charts.WithXAxisOpts(opts.XAxis{
			Name: "时间",
		}),
	)

	// 添加X轴数据
	line.SetXAxis(timeData)

	// 准备错误率数据
	errorRateData := make([]opts.LineData, len(stats))

	for i, stat := range stats {
		errorRateData[i] = opts.LineData{Value: stat.ErrorRate}
	}

	// 添加错误率数据系列
	line.AddSeries("错误率", errorRateData).
		SetSeriesOptions(
			charts.WithLineChartOpts(opts.LineChart{
				Smooth: opts.Bool(true),
			}),
			charts.WithMarkPointNameTypeItemOpts(
				opts.MarkPointNameTypeItem{Name: "最大值", Type: "max"},
			),
			charts.WithMarkLineNameYAxisItemOpts(
				opts.MarkLineNameYAxisItem{Name: "警戒线", YAxis: 1.0},
			),
		)

	return line
}

// 生成总结页面
func (v *Visualizer) generateSummaryPage(finalStats load.Stats, params map[string]string) {
	// 创建HTML内容
	// 生成参数表格行
	paramsRows := ""
	for key, value := range params {
		paramsRows += fmt.Sprintf(`
			<div class="arg-box">
                <div class="stat-label">%s</div>
                <div class="arg-value">%s</div>
            </div>`, key, value)
	}

	htmlContent := fmt.Sprintf(`
	<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>RaftKV Benchmark</title>
    <style>
        body {
            font-family: 'Segoe UI', 'Source Han Sans CN', 'Microsoft YaHei', sans-serif;
            color: #333;
            background-color: #f9f9f9;
			padding: 20px;
			margin: 0 auto;
			max-width: 1000px;
        }
        .container {
            max-width: 1000px;
            margin: 0 auto;
            background-color: white;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0, 0, 0, 0.05);
            padding: 30px;
        }
        .header {
            text-align: center;
            border-bottom: 1px solid #eaeaea;
            margin-bottom: 30px;
        }
        .header h1 {
            color: #2c5282;
            font-size: 32px;
			margin: 0 auto 24px;
        }
        .header p {
            color: #718096;
            font-size: 14px;
        }
        h2 {
            color: #2c5282;
            font-size: 20px;
            font-weight: 600;
            margin: 40px;
            text-align: center;
            position: relative;
        }
        h2:after {
            content: "";
            position: absolute;
            bottom: -8px;
            left: 50%%;
            transform: translateX(-50%%);
            width: 60px;
            height: 3px;
            background-color: #4299e1;
        }
        .summary-stats {
            display: grid;
            grid-template-columns: repeat(3, minmax(200px, 1fr));
            gap: 15px;
            margin: 25px auto;
			width: 80%%;
        }
        .stat-box {
            background-color: #f8fafc;
            border-radius: 6px;
            padding: 20px;
            text-align: center;
            transition: transform 0.2s, box-shadow 0.2s;
            box-shadow: 0 2px 5px rgba(0,0,0,0.05);
        }
        .stat-box:hover {
            transform: translateY(-3px);
            box-shadow: 0 4px 12px rgba(0, 0, 0, 0.08);
        }
        .arg-box {
            background-color: #f8fafc;
            border-radius: 6px;
            padding: 15px;
            text-align: center;
            transition: transform 0.2s, box-shadow 0.2s;
            box-shadow: 0 2px 5px rgba(0,0,0,0.05);
        }
        .arg-box:hover {
            transform: translateY(-3px);
            box-shadow: 0 4px 12px rgba(0, 0, 0, 0.08);
        }
        .stat-label, .arg-label {
            color: #718096;
            font-size: 14px;
            margin-bottom: 8px;
        }
        .arg-value {
            font-size: 20px;
            font-weight: 600;
            color: #2d3748;
            margin: 10px 0;
        }
        .stat-value {
            font-size: 24px;
            font-weight: 600;
            color: #2d3748;
            margin: 10px 0;
        }
        /* 主要性能指标突出显示 */
        .primary-stat {
            background-color: #ebf8ff;
            border-left: 4px solid #4299e1;
        }
        .charts-section {
            margin-top: 40px;
            border-top: 1px solid #eaeaea;
            padding-top: 30px;
        }
        .chart-links {
            display: flex;
            flex-wrap: wrap;
            justify-content: center;
            gap: 15px;
            margin: 20px 0;
        }
        .chart-link {
            margin: 10px;
            padding: 12px 20px;
            background-color: #4299e1;
            color: white;
            text-decoration: none;
            border-radius: 6px;
            transition: all 0.3s ease;
            font-weight: 500;
        }
        .chart-link:hover {
            background-color: #3182ce;
            transform: translateY(-2px);
        }
        .footer {
            text-align: center;
            margin-top: 40px;
            padding-top: 20px;
            border-top: 1px solid #eaeaea;
            color: #718096;
            font-size: 14px;
        }
        @media (max-width: 768px) {
            .summary-stats {
                grid-template-columns: repeat(2, 1fr);
            }
        }
        @media (max-width: 480px) {
            .summary-stats {
                grid-template-columns: 1fr;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>RaftKV Benchmark</h1>
        </div>
        
        <h2>运行参数</h2>
        <div class="summary-stats">
            %s
        </div>
        
        <h2>运行结果</h2>
        <div class="summary-stats">
            <div class="stat-box primary-stat">
                <div class="stat-label">总请求数</div>
                <div class="stat-value">%d</div>
            </div>
            <div class="stat-box primary-stat">
                <div class="stat-label">每秒查询数 (QPS)</div>
                <div class="stat-value">%.2f</div>
            </div>
            <div class="stat-box primary-stat">
                <div class="stat-label">平均延迟</div>
                <div class="stat-value">%.2f <span style="font-size: 14px; color: #718096;">ms</span></div>
            </div>
			<div class="stat-box primary-stat">
                <div class="stat-label">P50 延迟</div>
                <div class="stat-value">%.2f <span style="font-size: 14px; color: #718096;">ms</span></div>
            </div>
			<div class="stat-box primary-stat">
                <div class="stat-label">P90 延迟</div>
                <div class="stat-value">%.2f <span style="font-size: 14px; color: #718096;">ms</span></div>
            </div>
        </div>
        
        <div class="charts-section">
            <iframe width="100%%" height="3200px" src="performance_charts.html" style="border: none; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.05);">
            </iframe>
        </div>
        
        <div class="footer">
            <p>RaftKV 性能测试框架生成 - %s</p>
        </div>
    </div>
</body>
</html>
	`,
		paramsRows,
		finalStats.TotalOps,
		finalStats.QPS,
		finalStats.AvgLatency,
		finalStats.P50Latency,
		finalStats.P90Latency,
		time.Now().Format("2006-01-02"),
	)

	// 写入HTML文件
	indexFile := filepath.Join(v.resultDir, "index.html")
	err := os.WriteFile(indexFile, []byte(htmlContent), 0644)
	if err != nil {
		log.Printf("创建总结页面失败: %v", err)
	}
}
