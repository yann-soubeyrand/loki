package v1

import (
	"bufio"
	"fmt"
	"github.com/stretchr/testify/require"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
)

func listFilesRecursive(rootPath string) ([]string, error) {
	var files []string

	err := filepath.Walk(rootPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		//fmt.Println(info.Name())
		// Skip directories
		if info.IsDir() {
			return nil
		}
		// Add the file path to the slice
		files = append(files, path)
		return nil
	})

	return files, err
}

func TestReadingLocalFiles(t *testing.T) {
	var (
		//dir = "/Users/progers/baddat/loki_dev_006_index_19731/29/blooms/"
		dir = "/Users/progers/baddata2/loki_dev_006_index_19733/29/blooms/"
	)
	files, _ := listFilesRecursive(dir)
	for _, file := range files {
		cmd := exec.Command("mkdir", "/tmp/foo")
		_ = cmd.Run()
		fmt.Println(file)
		file, _ := os.Open(file)
		defer file.Close()
		reader := bufio.NewReader(file)
		UnTarGz("/tmp/foo", reader)
		r := NewDirectoryBlockReader("/tmp/foo")
		err := r.Init()
		require.NoError(t, err)

		_, err = r.Index()
		require.NoError(t, err)

		_, err = r.Blooms()
		require.NoError(t, err)

		block := NewBlock(r)
		blockQuerier := NewBlockQuerier(block)
		blockIters := NewPeekingIter[*SeriesWithBloom](blockQuerier)
		for blockIters.Next() {
			_ = blockIters.At()
		}
		blockQuerier.blooms.Next()

		cmd = exec.Command("rm", "-rf", "/tmp/foo")
		_ = cmd.Run()
	}
}

func TestEmptyFiles(t *testing.T) {

	cmd := exec.Command("mkdir", "/tmp/foo")
	_ = cmd.Run()
	cmd = exec.Command("touch", "/tmp/foo/bloom")
	_ = cmd.Run()
	cmd = exec.Command("touch", "/tmp/foo/series")
	_ = cmd.Run()

	r := NewDirectoryBlockReader("/tmp/foo")
	err := r.Init()
	require.NoError(t, err)

	_, err = r.Index()
	require.NoError(t, err)

	_, err = r.Blooms()
	require.NoError(t, err)

	block := NewBlock(r)
	blockQuerier := NewBlockQuerier(block)
	blockIters := NewPeekingIter[*SeriesWithBloom](blockQuerier)
	for blockIters.Next() {
		_ = blockIters.At()
	}
	blockQuerier.blooms.Next()

	cmd = exec.Command("rm", "-rf", "/tmp/foo")
	_ = cmd.Run()

}

/*
	func TestReadingLocalFilesViaShipper(t *testing.T) {
		var (
			//dir = "/Users/progers/baddat/loki_dev_006_index_19731/29/blooms/"
			dir = "/Users/progers/baddata2/loki_dev_006_index_19733/29/blooms/"
		)
		files, _ := listFilesRecursive(dir)
		for _, file := range files {
			cmd := exec.Command("mkdir", "/tmp/foo")
			_ = cmd.Run()
			fmt.Println(file)
			file, _ := os.Open(file)
			defer file.Close()
			reader := bufio.NewReader(file)
			UnTarGz("/tmp/foo", reader)
			r := NewDirectoryBlockReader("/tmp/foo")
			err := r.Init()
			require.NoError(t, err)

			_, err = r.Index()
			require.NoError(t, err)

			_, err = r.Blooms()
			require.NoError(t, err)

			block := NewBlock(r)
			blockQuerier := NewBlockQuerier(block)
			blockIters := NewPeekingIter[*SeriesWithBloom](blockQuerier)
			for blockIters.Next() {
				_ = blockIters.At()
			}
			blockQuerier.blooms.Next()

			cmd = exec.Command("rm", "-rf", "/tmp/foo")
			_ = cmd.Run()
		}
	}

	func TestReadingAllLocalFiles(t *testing.T) {
		var (
			//dir = "/Users/progers/baddat/loki_dev_006_index_19731/29/blooms/"
			dir = "/Users/progers/baddata2/loki_dev_006_index_19733/29/blooms/"
		)
		cmd := exec.Command("mkdir", "/tmp/foo")
		_ = cmd.Run()
		files, _ := listFilesRecursive(dir)
		blockIters := make([]PeekingIterator[*SeriesWithBloom], len(files))
		for i, file := range files {
			tmpDirI := "/tmp/foo/" + strconv.Itoa(i)
			cmd := exec.Command("mkdir", tmpDirI)
			_ = cmd.Run()
			fmt.Println(file)
			file, _ := os.Open(file)
			defer file.Close()
			reader := bufio.NewReader(file)
			UnTarGz(tmpDirI, reader)
			r := NewDirectoryBlockReader(tmpDirI)
			err := r.Init()
			require.NoError(t, err)

			_, err = r.Index()
			require.NoError(t, err)

			_, err = r.Blooms()
			require.NoError(t, err)

			block := NewBlock(r)
			blockQuerier := NewBlockQuerier(block)
			blockIters[i] = NewPeekingIter[*SeriesWithBloom](blockQuerier)

		}
		heap := NewHeapIterForSeriesWithBloom(blockIters...)
		fmt.Printf("made heap iterator\n")
		_ = heap.Next()
		fmt.Println("Got here")
		cmd = exec.Command("rm", "-rf", "/tmp/foo")
		_ = cmd.Run()
	}

	func TestReadingAllLocalFilesAndDoMore(t *testing.T) {
		var (
			//dir = "/Users/progers/baddat/loki_dev_006_index_19731/29/blooms/"
			dir = "/Users/progers/baddata2/loki_dev_006_index_19733/29/blooms/"
		)
		cmd := exec.Command("mkdir", "/tmp/foo")
		_ = cmd.Run()
		files, _ := listFilesRecursive(dir)
		blockIters := make([]PeekingIterator[*SeriesWithBloom], len(files))
		for i, file := range files {
			tmpDirI := "/tmp/foo/" + strconv.Itoa(i)
			cmd := exec.Command("mkdir", tmpDirI)
			_ = cmd.Run()
			fmt.Println(file)
			file, _ := os.Open(file)
			defer file.Close()
			reader := bufio.NewReader(file)
			UnTarGz(tmpDirI, reader)
			r := NewDirectoryBlockReader(tmpDirI)
			err := r.Init()
			require.NoError(t, err)

			_, err = r.Index()
			require.NoError(t, err)

			_, err = r.Blooms()
			require.NoError(t, err)

			block := NewBlock(r)
			blockQuerier := NewBlockQuerier(block)
			blockIters[i] = NewPeekingIter[*SeriesWithBloom](blockQuerier)

		}
		seriesFromSeriesMeta := make([]*Series, 0)
		seriesIter := NewSliceIter(seriesFromSeriesMeta)
		populate := createPopulateFunc()
		blockOptions := NewBlockOptions(4, 0)
		mergeBlockBuilder, _ := NewPersistentBlockBuilder("/tmp/foo", blockOptions)
		//mergedBlocks := NewPeekingIter[*SeriesWithBloom](NewHeapIterForSeriesWithBloom(blockIters...))
		mergeBuilder := NewMergeBuilder(
			blockIters,
			seriesIter,
			populate)

		fmt.Printf("made merge builder\n")
		_, _ = mergeBlockBuilder.MergeBuild(mergeBuilder)
		fmt.Printf("did merge build\n")

		cmd = exec.Command("rm", "-rf", "/tmp/foo")
		_ = cmd.Run()
	}

	func TestReadingAllLocalFilesAndDoMoreWithSeries(t *testing.T) {
		var (
			dir = "/Users/progers/baddata2/loki_dev_006_index_19733/29/blooms/"
		)

		_ = os.MkdirAll("/tmp/foo", os.ModePerm)
		files, _ := listFilesRecursive(dir)
		blockIters := make([]PeekingIterator[*SeriesWithBloom], len(files))
		for i, file := range files {
			tmpDirI := "/tmp/foo/" + strconv.Itoa(i)
			_, err := os.Stat(tmpDirI)
			//fmt.Printf("File %d is %s\n", i, file)
			// Check if the error is due to the directory not existing
			if os.IsNotExist(err) {
				cmd := exec.Command("mkdir", tmpDirI)
				_ = cmd.Run()
				file, _ := os.Open(file)
				defer file.Close()
				reader := bufio.NewReader(file)
				UnTarGz(tmpDirI, reader)
			}
			r := NewDirectoryBlockReader(tmpDirI)
			block := NewBlock(r)
			blockQuerier := NewBlockQuerier(block)
			blockIters[i] = NewPeekingIter[*SeriesWithBloom](blockQuerier)
		}

		seriesFromSeriesMeta := make([]*Series, 2)
		series := &Series{
			Fingerprint: 1,
			Chunks:      nil,
		}
		seriesFromSeriesMeta[0] = series
		seriesFromSeriesMeta[1] = series
		seriesIter := NewSliceIter(seriesFromSeriesMeta)
		populate := createPopulateFunc()
		blockOptions := NewBlockOptions(4, 0)
		_, _ = NewPersistentBlockBuilder("/tmp/foo", blockOptions) //mergeBlockBuilder
		_ = NewMergeBuilder(                                       //mergeBuilder
			blockIters,
			seriesIter,
			populate)

		//mergedBlocks := NewPeekingIter[*SeriesWithBloom](NewHeapIterForSeriesWithBloom(blockIters...))
		mergedBlocks := NewHeapIterForSeriesWithBloom(blockIters...)


		for mergedBlocks.Next() {
			mergedBlocks.At()
		}
	}
*/
func TestReadingAllLocalFilesAndFigureOutTheIterators(t *testing.T) {
	var (
		dir = "/Users/progers/baddata2/loki_dev_006_index_19733/29/blooms/"
	)

	_ = os.MkdirAll("/tmp/foo", os.ModePerm)
	files, _ := listFilesRecursive(dir)
	blockIters := make([]PeekingIterator[*SeriesWithBloom], len(files))
	for i, file := range files {
		tmpDirI := "/tmp/foo/" + strconv.Itoa(i)
		_, err := os.Stat(tmpDirI)
		// Check if the error is due to the directory not existing
		if os.IsNotExist(err) {
			cmd := exec.Command("mkdir", tmpDirI)
			_ = cmd.Run()
			file, _ := os.Open(file)
			defer file.Close()
			reader := bufio.NewReader(file)
			UnTarGz(tmpDirI, reader)
		}
		r := NewDirectoryBlockReader(tmpDirI)
		block := NewBlockWithIndex(r, i)
		blockQuerier := NewBlockQuerierWithIndex(block, i)
		blockIters[i] = NewPeekingIterWithIndex[*SeriesWithBloom](blockQuerier, i)
	}

	mergedBlocks := NewHeapIterForSeriesWithBloom(blockIters...)

	/*
		for _, itr := range blockIters {
			for itr.Next() {
				itr.At()
			}
		}
	*/

	for mergedBlocks.Next() {
		mergedBlocks.At()
	}

}

func createPopulateFunc() func(series *Series, bloom *Bloom) error {
	return func(series *Series, bloom *Bloom) error {
		return nil
	}
}

// Redeclared here since this is a test file and we get an import loop otherwise
type PersistentBlockBuilder struct {
	builder  *BlockBuilder
	localDst string
}

func NewPersistentBlockBuilder(localDst string, blockOptions BlockOptions) (*PersistentBlockBuilder, error) {
	// write bloom to a local dir
	b, err := NewBlockBuilder(blockOptions, NewDirectoryBlockWriter(localDst))
	if err != nil {
		return nil, err
	}
	builder := PersistentBlockBuilder{
		builder:  b,
		localDst: localDst,
	}
	return &builder, nil
}

func (p *PersistentBlockBuilder) BuildFrom(itr Iterator[SeriesWithBloom]) (uint32, error) {
	return p.builder.BuildFrom(itr)
}

func (p *PersistentBlockBuilder) MergeBuild(builder *MergeBuilder) (uint32, error) {
	return builder.Build(p.builder)
}

func (p *PersistentBlockBuilder) Data() (io.ReadSeekCloser, error) {
	blockFile, err := os.Open(filepath.Join(p.localDst, BloomFileName))
	if err != nil {
		return nil, err
	}
	return blockFile, nil
}
