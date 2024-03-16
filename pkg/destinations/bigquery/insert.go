func (s *BigQueryServer) CreateEmptyTable(table string) error {
	sql := fmt.Sprintf("CREATE TABLE IF NOT EXISTS \"%s\" (__row_id BIGINT)", table)
	
	return err
}

func (s *BigQueryServer) CreateColumns(table string, fileName string) error {
	input, err := os.Open(fileName)
	
	err = input.Close()
	if err != nil {
		log.Error().Err(err).Str("filename", fileName).Msg("Unable to close file")
	}
}

func (s *BigQueryServer) InsertFromNDJsonFile(table string, fileName string) error {
}