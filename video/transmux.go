package video

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"github.com/grafov/m3u8"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	ffmpeg "github.com/u2takey/ffmpeg-go"
)

const (
	Mp4DurationLimit = 21600 //MP4s will be generated only for first 6 hours
	MaxArgLimit      = 250
)

func MuxTStoMP4(tsInputFile, mp4OutputFile string) ([]string, error) {
	var transmuxOutputFiles []string
	// transmux the .ts file into a standalone MP4 file
	ffmpegErr := bytes.Buffer{}
	err := ffmpeg.Input(tsInputFile).
		Output(mp4OutputFile, ffmpeg.KwArgs{
			"analyzeduration": "15M",           // Analyze up to 15s of video to figure out the format. We saw failures to detect the video codec without this
			"movflags":        "faststart",     // Need this for progressive playback and probing
			"c":               "copy",          // Don't accidentally transcode
			"bsf:a":           "aac_adtstoasc", // Remove ADTS header (required for ts -> mp4 container conversion)
		}).
		OverWriteOutput().WithErrorOutput(&ffmpegErr).Run()
	if err != nil {
		return nil, fmt.Errorf("failed to transmux concatenated mpeg-ts file (%s) into a mp4 file [%s]: %w", tsInputFile, ffmpegErr.String(), err)
	}
	// Verify the mp4 output file was created
	_, err = os.Stat(mp4OutputFile)
	if err != nil {
		return nil, fmt.Errorf("transmux error: failed to stat MP4 media file: %w", err)
	} else {
		transmuxOutputFiles = append(transmuxOutputFiles, mp4OutputFile)
	}
	return transmuxOutputFiles, nil
}

func MuxTStoFMP4(fmp4ManifestOutputFile string, inputs ...string) error {
	baseFragMp4Dir := filepath.Dir(fmp4ManifestOutputFile)
	err := os.Mkdir(baseFragMp4Dir, 0700)
	if err != nil && !os.IsExist(err) {
		return fmt.Errorf("transmux error: failed to create subdir to output fmp4 files: %w", err)
	}

	var args []string
	mapArgs := []string{"-map", "0:a"}
	for i, input := range inputs {
		args = append(args, "-i", input)
		mapArgs = append(mapArgs, "-map", fmt.Sprintf("%d:v", i))
	}
	args = append(args,
		"-movflags", "frag_keyframe+empty_moov",
		"-c", "copy",
		"-bsf:a", "aac_adtstoasc",
		"-f", "dash",
		"-dash_segment_type", "mp4",
		"-single_file", "1",
		"-hls_playlist", "1",
		"-hls_time", "10",
		"-hls_playlist_type", "vod",
		"-hls_segment_type", "fmp4",
		// unfortunately I had to hard-code these, ffmpeg throws an error with our ts input otherwise
		// similar to https://stackoverflow.com/questions/48005093/ffmpeg-incompatible-with-output-codec-id
		// there doesn't seem to be a way for ffmpeg to work out the tags automatically,
		// if our codecs change we'll need to update these
		"-vtag", "avc1",
		"-atag", "mp4a",
	)
	args = append(args, mapArgs...)
	args = append(args, fmp4ManifestOutputFile)

	timeout, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(timeout, "ffmpeg", args...)

	var outputBuf bytes.Buffer
	var stdErr bytes.Buffer
	cmd.Stdout = &outputBuf
	cmd.Stderr = &stdErr

	err = cmd.Run()
	if err != nil {
		return fmt.Errorf("error running ffmpeg [%s] [%s] %w", outputBuf.String(), stdErr.String(), err)
	}

	_, err = os.Stat(fmp4ManifestOutputFile)
	if err != nil {
		return fmt.Errorf("transmux error: failed to stat FMP4 Dash manifest [%s] [%s]: %w", outputBuf.String(), stdErr.String(), err)
	}
	_, err = os.Stat(filepath.Join(baseFragMp4Dir, "master.m3u8"))
	if err != nil {
		return fmt.Errorf("transmux error: failed to stat FMP4 HLS manifest [%s] [%s]: %w", outputBuf.String(), stdErr.String(), err)
	}
	return nil
}

func ConcatTS(tsFileName string, segmentsList *TSegmentList, sourceMediaPlaylist m3u8.MediaPlaylist, useStreamBasedConcat bool) (int64, error) {
	// Used to track total bytes concatenated will match total bytes transcoded
	var totalBytes int64
	// Used to ensure total duration of segments processed does not exceed Mp4DurationLimit
	var totalDuration float64
	// Strip the '.ts' from filename
	fileBaseWithoutExt := tsFileName[:len(tsFileName)-len(".ts")]
	// Save a list of segment filenames so that we can delete them once done
	segmentFilenames := []string{}
	defer func() {
		for _, f := range segmentFilenames {
			os.Remove(f)
		}
	}()

	if !useStreamBasedConcat {
		// Add segment filename to the text file
		for segName := range segmentsList.GetSortedSegments() {
			// Check each segment that was written to disk in the disk-writing goroutine
			segmentFilename := fileBaseWithoutExt + "_" + strconv.Itoa(segName) + ".ts"
			fileInfo, err := os.Stat(segmentFilename)
			if err != nil {
				return totalBytes, fmt.Errorf("error stat segment %s  err: %w", segmentFilename, err)
			}
			segBytes := fileInfo.Size()

			segmentFilenames = append(segmentFilenames, segmentFilename)
			totalBytes = totalBytes + int64(segBytes)

			// Check total duration processed so far and stop concatenating if Mp4DurationLimit is reached
			// i.e. generate MP4s for only up to duration specified by Mp4DurationLimit
			segDuration := sourceMediaPlaylist.Segments[segName].Duration
			totalDuration = totalDuration + segDuration
			if totalDuration > Mp4DurationLimit {
				break
			}
		}
		// If the argument list of files gets too long, linux might complain about exceeding
		// MAX_ARG limit and ffmpeg (or any other command) using the long list will fail to run.
		// So we split into chunked files then concat it one final time to get the final file.
		if len(segmentFilenames) > MaxArgLimit {
			chunks := ConcatChunkedFiles(segmentFilenames, MaxArgLimit)

			var chunkFiles []string
			for idx, chunk := range chunks {
				concatArg := "concat:" + strings.Join(chunk, "|")
				chunkFilename := fileBaseWithoutExt + "_" + "chunk" + strconv.Itoa(idx) + ".ts"
				chunkFiles = append(chunkFiles, chunkFilename)
				err := concatFiles(concatArg, chunkFilename)
				if err != nil {
					return totalBytes, fmt.Errorf("failed to file-concat a chunk (#%d)into a ts file: %w", idx, err)
				}
			}
			if len(chunkFiles) == 0 {
				return totalBytes, fmt.Errorf("failed to generate chunks to concat")
			}
			// override with the chunkFilenames instead
			segmentFilenames = chunkFiles

		}
		concatArg := "concat:" + strings.Join(segmentFilenames, "|")

		// Use file-based concatenation by reading segment files in text file
		err := concatFiles(concatArg, tsFileName)
		if err != nil {
			return totalBytes, fmt.Errorf("failed to file-concat into a ts file: %w", err)
		}

		return totalBytes, nil
	} else {
		// Create a text file containing filenames of the segments
		segmentListTxtFileName := fileBaseWithoutExt + ".txt"
		segmentListTxtFile, err := os.Create(segmentListTxtFileName)
		if err != nil {
			return totalBytes, fmt.Errorf("error creating  segment text file (%s) err: %w", segmentListTxtFileName, err)
		}
		defer segmentListTxtFile.Close()
		defer os.Remove(segmentListTxtFileName)
		w := bufio.NewWriter(segmentListTxtFile)

		// Add segment filename to the text file
		for segName := range segmentsList.GetSortedSegments() {
			// Check each segment that was written to disk in the disk-writing goroutine
			segmentFilename := fileBaseWithoutExt + "_" + strconv.Itoa(segName) + ".ts"
			fileInfo, err := os.Stat(segmentFilename)
			if err != nil {
				return totalBytes, fmt.Errorf("error stat segment %s  err: %w", segmentFilename, err)
			}
			segBytes := fileInfo.Size()

			segmentFilenames = append(segmentFilenames, segmentFilename)
			totalBytes = totalBytes + int64(segBytes)
			// Add filename to the text file
			line := fmt.Sprintf("file '%s'\n", segmentFilename)
			if _, err = w.WriteString(line); err != nil {
				return totalBytes, fmt.Errorf("error writing segment %d to text file err: %w", segName, err)
			}
			// Flush to make sure all buffered operations are applied
			if err = w.Flush(); err != nil {
				return totalBytes, fmt.Errorf("error flushing text file %s err: %w", segmentFilename, err)
			}

			// Check total duration processed so far and stop concatenating if Mp4DurationLimit is reached
			// i.e. generate MP4s for only up to duration specified by Mp4DurationLimit
			segDuration := sourceMediaPlaylist.Segments[segName].Duration
			totalDuration = totalDuration + segDuration
			if totalDuration > Mp4DurationLimit {
				break
			}

		}

		// Use stream-based concatenation by reading segment files in text file
		err = concatStreams(segmentListTxtFileName, tsFileName)
		if err != nil {
			return totalBytes, fmt.Errorf("failed to stream-concat %s into a ts file: %w", segmentListTxtFileName, err)
		}

		return totalBytes, nil
	}
}

func concatStreams(segmentList, outputTsFileName string) error {
	// Create a .ts file for a given rendition
	tsFile, err := os.Create(outputTsFileName)
	if err != nil {
		return fmt.Errorf("error creating file (%s) err: %w", outputTsFileName, err)
	}
	defer tsFile.Close()
	// Transmux the individual .ts files into a combined single ts file using stream based concatenation
	ffmpegErr := bytes.Buffer{}
	err = ffmpeg.Input(segmentList, ffmpeg.KwArgs{
		"f":    "concat", // Use stream based concatenation (instead of file based concatenation)
		"safe": "0"}).    // Must be 0 since relative paths to segments are used in segmentListTxtFileName
		Output(outputTsFileName, ffmpeg.KwArgs{
			"c": "copy", // Don't accidentally transcode
		}).
		OverWriteOutput().WithErrorOutput(&ffmpegErr).Run()
	if err != nil {
		return fmt.Errorf("failed to transmux multiple ts files from %s into a ts file [%s]: %w", segmentList, ffmpegErr.String(), err)
	}
	// Verify the ts output file was created
	_, err = os.Stat(outputTsFileName)
	if err != nil {
		return fmt.Errorf("transmux error: failed to stat .ts media file: %w", err)
	}
	return nil
}

func concatFiles(segmentList, outputTsFileName string) error {
	// Create a .ts file for a given rendition
	tsFile, err := os.Create(outputTsFileName)
	if err != nil {
		return fmt.Errorf("error creating file (%s) err: %w", outputTsFileName, err)
	}
	defer tsFile.Close()
	// Transmux the individual .ts files into a combined single ts file using file based concatenation
	ffmpegErr := bytes.Buffer{}
	err = ffmpeg.Input(segmentList).
		Output(outputTsFileName, ffmpeg.KwArgs{
			"c": "copy", // Don't accidentally transcode
		}).
		OverWriteOutput().WithErrorOutput(&ffmpegErr).Run()
	if err != nil {
		return fmt.Errorf("failed to transmux multiple ts files from %s into a ts file [%s]: %w", segmentList, ffmpegErr.String(), err)
	}
	// Verify the ts output file was created
	_, err = os.Stat(outputTsFileName)
	if err != nil {
		return fmt.Errorf("transmux error: failed to stat .ts media file: %w", err)
	}
	return nil
}

// ConcatChunkedFiles splits the segmentFilenames into smaller chunks based on the maxLength value,
// where maxLength is the maximum number of filenames per chunk.
func ConcatChunkedFiles(filenames []string, maxLength int) [][]string {
	var chunks [][]string
	for maxLength > 0 && len(filenames) > 0 {
		if len(filenames) <= maxLength {
			chunks = append(chunks, filenames)
			break
		}
		chunks = append(chunks, filenames[:maxLength])
		filenames = filenames[maxLength:]
	}
	return chunks
}
