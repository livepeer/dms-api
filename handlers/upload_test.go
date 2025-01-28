package handlers

import (
	"fmt"
	"testing"

	"github.com/livepeer/catalyst-api/video"
	"github.com/stretchr/testify/require"
)

func TestGetTargetOutputs(t *testing.T) {
	tests := []struct {
		name                  string
		req                   UploadVODRequest
		expectedHlsURL        string
		expectedMp4URL        string
		expectedFragMp4URL    string
		expectedClipURL       string
		expectedMp4ShortOnly  bool
		expectedThumbnailsURL string
	}{
		{
			name: "single output location with HLS",
			req: UploadVODRequest{OutputLocations: []UploadVODRequestOutputLocation{{
				URL: "s3+https://user:pass@bucket",
				Outputs: UploadVODRequestOutputLocationOutputs{
					HLS: "enabled",
				}}}},
			expectedHlsURL:     "s3+https://user:pass@bucket",
			expectedMp4URL:     "",
			expectedFragMp4URL: "",
		},
		{
			name: "no location with HLS",
			req: UploadVODRequest{OutputLocations: []UploadVODRequestOutputLocation{{
				URL: "s3+https://user:pass@bucket",
				Outputs: UploadVODRequestOutputLocationOutputs{
					MP4: "enabled",
				}}}},
			expectedHlsURL:     "",
			expectedMp4URL:     "s3+https://user:pass@bucket",
			expectedFragMp4URL: "",
		},
		{
			name: "multiple output locations, one with source segments",
			req: UploadVODRequest{OutputLocations: []UploadVODRequestOutputLocation{
				{
					URL: "s3+https://first:first@bucket",
					Outputs: UploadVODRequestOutputLocationOutputs{
						HLS: "enabled",
					},
				},
				{
					URL: "s3+https://second:second@bucket",
					Outputs: UploadVODRequestOutputLocationOutputs{
						MP4: "only_short",
					},
				},
				{
					URL: "s3+https://third:third@bucket",
					Outputs: UploadVODRequestOutputLocationOutputs{
						FragmentedMP4: "enabled",
					},
				},
				{
					URL: "s3+https://fourth:fourth@bucket",
					Outputs: UploadVODRequestOutputLocationOutputs{
						Clip: "enabled",
					},
				},
				{
					URL: "s3+https://fifth:fifth@bucket",
					Outputs: UploadVODRequestOutputLocationOutputs{
						Thumbnails: "enabled",
					},
				},
			}},
			expectedHlsURL:        "s3+https://first:first@bucket",
			expectedMp4URL:        "s3+https://second:second@bucket",
			expectedFragMp4URL:    "s3+https://third:third@bucket",
			expectedClipURL:       "s3+https://fourth:fourth@bucket",
			expectedThumbnailsURL: "s3+https://fifth:fifth@bucket",
			expectedMp4ShortOnly:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotHls := tt.req.getTargetOutput(func(o UploadVODRequestOutputLocationOutputs) string {
				return o.HLS
			})
			require.Equal(t, tt.expectedHlsURL, gotHls.URL)
			gotMp4, gotShortOnly := tt.req.getTargetMp4Output()
			gotFragMp4 := tt.req.getTargetOutput(func(o UploadVODRequestOutputLocationOutputs) string {
				return o.FragmentedMP4
			})
			gotClip := tt.req.getTargetOutput(func(o UploadVODRequestOutputLocationOutputs) string {
				return o.Clip
			})
			gotThumbs := tt.req.getTargetOutput(func(o UploadVODRequestOutputLocationOutputs) string {
				return o.Thumbnails
			})
			require.Equal(t, tt.expectedMp4URL, gotMp4.URL)
			require.Equal(t, tt.expectedMp4ShortOnly, gotShortOnly)
			require.Equal(t, tt.expectedFragMp4URL, gotFragMp4.URL)
			require.Equal(t, tt.expectedClipURL, gotClip.URL)
			require.Equal(t, tt.expectedThumbnailsURL, gotThumbs.URL)
		})
	}
}

func TestItRejectsLocalDomain(t *testing.T) {
	err := CheckSourceURLValid("http://ipfs.libraries.svc.cluster.local:8080/ipfs/asdasd")
	require.EqualError(t, err, ".local domains are not valid")
}

func TestItRejectsEmptyURL(t *testing.T) {
	err := CheckSourceURLValid("")
	require.EqualError(t, err, "empty source URL")
}

func TestItAcceptsValidSourceURLs(t *testing.T) {
	require.NoError(t, CheckSourceURLValid("http://www.google.com"))
	require.NoError(t, CheckSourceURLValid("http://www.google.com:8080/123/asdsdf"))
	require.NoError(t, CheckSourceURLValid("ipfs://sfsdf234fdsdfsd"))
	require.NoError(t, CheckSourceURLValid("ar://123456"))
}

func TestIsProfileValid(t *testing.T) {
	tests := []struct {
		name     string
		request  UploadVODRequest
		expected error
	}{
		{
			name: "ValidProfiles",
			request: UploadVODRequest{
				Profiles: []video.EncodedProfile{
					{Width: 1920, Height: 1080, Bitrate: 5000, FPS: 30},
					{Width: 1280, Height: 720, Bitrate: 2000, FPS: 24},
				},
			},
			expected: nil,
		},
		{
			name: "InvalidProfileWidthWithoutHeight",
			request: UploadVODRequest{
				Profiles: []video.EncodedProfile{
					{Name: "InvalidProfileWidthWithoutHeight", Width: 0, Height: 720, Bitrate: 2000, FPS: 24},
				},
			},
			expected: fmt.Errorf("if multiple profiles are specified, all must have a Width, Height and Bitrate. Profile \"InvalidProfileWidthWithoutHeight\" did not"),
		},
		{
			name: "InvalidProfileHeightWithoutWidth",
			request: UploadVODRequest{
				Profiles: []video.EncodedProfile{
					{Name: "InvalidProfileHeightWithoutWidth", Width: 1920, Height: 0, Bitrate: 5000, FPS: 30},
				},
			},
			expected: fmt.Errorf("if multiple profiles are specified, all must have a Width, Height and Bitrate. Profile \"InvalidProfileHeightWithoutWidth\" did not"),
		},
		{
			name: "InvalidProfileWidthHeightWithoutBitrate",
			request: UploadVODRequest{
				Profiles: []video.EncodedProfile{
					{Name: "InvalidProfileWidthHeightWithoutBitrate", Width: 1920, Height: 1080, Bitrate: 0, FPS: 30},
				},
			},
			expected: fmt.Errorf("if multiple profiles are specified, all must have a Width, Height and Bitrate. Profile \"InvalidProfileWidthHeightWithoutBitrate\" did not"),
		},
		{
			name: "SingleProfileWithNonZeroBitrate",
			request: UploadVODRequest{
				Profiles: []video.EncodedProfile{
					{Bitrate: 2000},
				},
			},
			expected: nil,
		},
		{
			name: "SingleProfileWithZeroBitrate",
			request: UploadVODRequest{
				Profiles: []video.EncodedProfile{
					{Name: "SingleProfileWithZeroBitrate", Bitrate: 0},
				},
			},
			expected: fmt.Errorf("without Width or Height specified, Bitrate must be set"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := test.request.CheckProfileValid()
			if test.expected == nil {
				require.NoError(t, result)
			} else {
				require.EqualError(t, test.expected, result.Error())
			}
		})
	}
}

func TestWeCanDetermineIfItsAClippingRequest(t *testing.T) {
	u := UploadVODRequest{
		ClipStrategy: video.ClipStrategy{
			PlaybackID: "12345",
		},
	}
	require.True(t, u.IsClippingRequest())

	u = UploadVODRequest{}
	require.False(t, u.IsClippingRequest())
}

func TestWeRejectInvalidClippingRequests(t *testing.T) {
	u := UploadVODRequest{
		OutputLocations: []UploadVODRequestOutputLocation{
			{
				URL:  "some-url",
				Type: "clip",
				Outputs: UploadVODRequestOutputLocationOutputs{
					Clip: "enabled",
				},
			},
		},
	}

	u.ClipStrategy.StartTime = 0
	u.ClipStrategy.EndTime = 0
	require.EqualError(t, u.ValidateClippingRequest(), "clip start time and end time were both 0 but should be different")

	u.ClipStrategy.StartTime = -1
	u.ClipStrategy.EndTime = 0
	require.EqualError(t, u.ValidateClippingRequest(), "clip start time -1 cannot be less than 0")

	u.ClipStrategy.StartTime = 123123123123
	u.ClipStrategy.EndTime = 1
	require.EqualError(t, u.ValidateClippingRequest(), "clip start time 123123123123 should be after end time 1")

	u.ClipStrategy.StartTime = 0
	u.ClipStrategy.EndTime = -1
	require.EqualError(t, u.ValidateClippingRequest(), "clip end time -1 cannot be less than 0")

	u.ClipStrategy.StartTime = 1722005308
	u.ClipStrategy.EndTime = 1722005309
	require.EqualError(t, u.ValidateClippingRequest(), "clip start time 1722005308 is in unix seconds, but should be milliseconds")

	u.ClipStrategy.StartTime = 1722005308000
	u.ClipStrategy.EndTime = 1722005309
	require.EqualError(t, u.ValidateClippingRequest(), "clip end time 1722005309 is in unix seconds, but should be milliseconds")
}
