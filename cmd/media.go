package cmd

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/zhangyf/objstore"
)

// MediaType 媒体类别
type MediaType string

const (
	MediaTypeImage   MediaType = "image"
	MediaTypeVideo   MediaType = "video"
	MediaTypeUnknown MediaType = ""
)

// MediaInfo 媒体元数据
type MediaInfo struct {
	Type   MediaType `json:"type"`
	Format string    `json:"format"`
	Width  int       `json:"width"`
	Height int       `json:"height"`

	// Video only
	VideoCodec     string  `json:"video_codec,omitempty"`
	FrameRate      float64 `json:"frame_rate,omitempty"`
	DurationSec    float64 `json:"duration_sec,omitempty"`
	Bitrate        int64   `json:"bitrate,omitempty"`
	AudioCodec     string  `json:"audio_codec,omitempty"`
	AudioChannels  int     `json:"audio_channels,omitempty"`
	AudioSampleRate int    `json:"audio_sample_rate,omitempty"`
}

// String 返回人类可读的摘要（用于 TYPE 列展示）
func (m *MediaInfo) String() string {
	switch m.Type {
	case MediaTypeImage:
		return fmt.Sprintf("IMAGE/%s %dx%d", strings.ToUpper(m.Format), m.Width, m.Height)
	case MediaTypeVideo:
		s := fmt.Sprintf("VIDEO/%s %dx%d %s", strings.ToUpper(m.Format), m.Width, m.Height, m.VideoCodec)
		if m.FrameRate > 0 {
			s += fmt.Sprintf("@%.0ffps", m.FrameRate)
		}
		if m.AudioCodec != "" {
			s += fmt.Sprintf(" %s", m.AudioCodec)
		}
		if m.AudioChannels > 0 {
			s += fmt.Sprintf(" %dch", m.AudioChannels)
		}
		if m.DurationSec > 0 {
			s += fmt.Sprintf(" %.0fs", m.DurationSec)
		}
		return s
	default:
		return "-"
	}
}

// ------------------------- COS CI 响应结构 -------------------------

// imageInfo 返回 JSON
type cosImageInfoResp struct {
	Format string `json:"format"`
	Width  string `json:"width"`
	Height string `json:"height"`
	Size   string `json:"size"`
}

// videoinfo (ci-process=videoinfo) 返回 XML
type cosVideoInfoXML struct {
	XMLName   xml.Name           `xml:"Response"`
	MediaInfo cosMediaInfoXML    `xml:"MediaInfo"`
}

type cosMediaInfoXML struct {
	Stream cosStreamXML `xml:"Stream"`
	Format cosFormatXML `xml:"Format"`
}

type cosStreamXML struct {
	Video cosVideoStreamXML `xml:"Video"`
	Audio cosAudioStreamXML `xml:"Audio"` // 可能为空元素 <Audio></Audio>
}

type cosVideoStreamXML struct {
	CodecName string `xml:"CodecName"`
	Width     string `xml:"Width"`
	Height    string `xml:"Height"`
	Fps       string `xml:"Fps"`
	Duration  string `xml:"Duration"`
	Bitrate   string `xml:"Bitrate"`
}

type cosAudioStreamXML struct {
	CodecName  string `xml:"CodecName"`
	SampleRate string `xml:"SampleRate"`
	Channels   string `xml:"Channels"`
}

type cosFormatXML struct {
	FormatName string `xml:"FormatName"`
	Duration   string `xml:"Duration"`
	Size       string `xml:"Size"`
	Bitrate    string `xml:"Bitrate"`
}

// ------------------------- 解析函数 -------------------------

func parseCOSImageInfo(data []byte) (*MediaInfo, error) {
	var resp cosImageInfoResp
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	if resp.Format == "" {
		return nil, fmt.Errorf("not an image")
	}
	w, _ := strconv.Atoi(resp.Width)
	h, _ := strconv.Atoi(resp.Height)
	return &MediaInfo{
		Type:   MediaTypeImage,
		Format: resp.Format,
		Width:  w,
		Height: h,
	}, nil
}

func parseCOSVideoInfo(data []byte, fileSize int64) (*MediaInfo, error) {
	var resp cosVideoInfoXML
	if err := xml.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	f := resp.MediaInfo.Format
	if f.FormatName == "" {
		return nil, fmt.Errorf("not a video")
	}

	mi := &MediaInfo{
		Type:   MediaTypeVideo,
		Format: shortFormatName(f.FormatName),
	}

	// 视频流（单对象，非数组）
	v := resp.MediaInfo.Stream.Video
	mi.Width, _ = strconv.Atoi(v.Width)
	mi.Height, _ = strconv.Atoi(v.Height)
	mi.VideoCodec = v.CodecName
	mi.FrameRate, _ = strconv.ParseFloat(v.Fps, 64)
	mi.DurationSec, _ = strconv.ParseFloat(v.Duration, 64)
	if v.Bitrate != "" {
		mi.Bitrate, _ = strconv.ParseInt(v.Bitrate, 10, 64)
	}

	// 音频流（可能为空元素 <Audio></Audio>）
	a := resp.MediaInfo.Stream.Audio
	if a.CodecName != "" {
		mi.AudioCodec = a.CodecName
		mi.AudioSampleRate, _ = strconv.Atoi(a.SampleRate)
		mi.AudioChannels, _ = strconv.Atoi(a.Channels)
	}

	// 时长回退到 Format 级别
	if mi.DurationSec == 0 {
		mi.DurationSec, _ = strconv.ParseFloat(f.Duration, 64)
	}

	// 码率：优先 stream 级别，回退 Format 级别，再回退计算值
	if mi.Bitrate == 0 && f.Bitrate != "" {
		mi.Bitrate, _ = strconv.ParseInt(f.Bitrate, 10, 64)
	}
	if mi.Bitrate == 0 && mi.DurationSec > 0 && fileSize > 0 {
		mi.Bitrate = int64(float64(fileSize) * 8 / mi.DurationSec)
	}

	return mi, nil
}

// shortFormatName 把 COS CI 返回的 FormatName 映射为简短名称
func shortFormatName(name string) string {
	parts := strings.SplitN(name, ",", 2)
	raw := parts[0]
	switch raw {
	case "mov":
		return "mp4"
	case "matroska":
		return "webm"
	default:
		return raw
	}
}

// ------------------------- 编排入口 -------------------------

const analyzeConcurrency = 10
const analyzeConfirmThreshold = 100

// AnalyzeMedia 通过 COS CI API 获取对象媒体信息。
// S3 provider 直接返回 nil（S3 无原生媒体分析能力）。
func AnalyzeMedia(store objstore.Store, ctx context.Context, key string, obj objstore.ObjectInfo) *MediaInfo {
	if store.Provider() != objstore.ProviderCOS {
		return nil
	}

	getter, ok := store.(objstore.ObjectQueryGetter)
	if !ok {
		return nil
	}

	// 1) 尝试 imageInfo
	mi := tryImageInfo(getter, ctx, key)
	if mi != nil {
		return mi
	}

	// 2) 尝试 videoinfo
	mi = tryVideoInfo(getter, ctx, key, obj.Size)
	return mi
}

func tryImageInfo(getter objstore.ObjectQueryGetter, ctx context.Context, key string) *MediaInfo {
	rc, err := getter.GetObjectWithQuery(ctx, key, "imageInfo")
	if err != nil {
		return nil
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		return nil
	}
	mi, err := parseCOSImageInfo(data)
	if err != nil {
		return nil
	}
	return mi
}

func tryVideoInfo(getter objstore.ObjectQueryGetter, ctx context.Context, key string, fileSize int64) *MediaInfo {
	rc, err := getter.GetObjectWithQuery(ctx, key, "ci-process=videoinfo")
	if err != nil {
		return nil
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		return nil
	}
	mi, err := parseCOSVideoInfo(data, fileSize)
	if err != nil {
		return nil
	}
	return mi
}
