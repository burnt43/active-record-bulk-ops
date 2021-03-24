require './test/initialize'

module ActiveRecord
  module BulkOps
    module Testing
      class InsertorTest < ActiveRecord::BulkOps::Testing::Test
        def test_set_all_columns_option
          foo01 = ActiveRecord::BulkOps::Testing::Foo.new(
            string01: 'string 1'
          )

          insertor01 = ActiveRecord::BulkOps::Insertion::Insertor.new(
            [foo01],
            set_all_columns: false
          )
          hash01 = insertor01.send(:insert_line_hash)

          insertor02 = ActiveRecord::BulkOps::Insertion::Insertor.new(
            [foo01],
            set_all_columns: true
          )
          hash02 = insertor02.send(:insert_line_hash)

          assert(hash01[0].start_with?('INSERT INTO `foos` (`string01`)'))
          assert(hash02[0].start_with?('INSERT INTO `foos` (`enum01`,`int01`,`string01`)'))
        end

        def test_insert_line_hash_with_enums
          foo01 = ActiveRecord::BulkOps::Testing::Foo.new(
            enum01:   'value01',
            int01:    1000,
            string01: 'string 1'
          )
          foo02 = ActiveRecord::BulkOps::Testing::Foo.new(
            enum01:   'value02',
            int01:    2000,
            string01: 'string 2'
          )
          foo03 = ActiveRecord::BulkOps::Testing::Foo.new(
            enum01:   'value03',
            int01:    3000,
            string01: 'string 3'
          )
          foo04 = ActiveRecord::BulkOps::Testing::Foo.new(
            enum01:   nil,
            int01:    nil,
            string01: nil
          )

          insertor = ActiveRecord::BulkOps::Insertion::Insertor.new([
            foo01,
            foo02,
            foo03,
            foo04
          ])

          hash = insertor.send(:insert_line_hash)

          assert_equal("INSERT INTO `foos` (`enum01`,`int01`,`string01`) VALUES (100,1000,'string 1'),(200,2000,'string 2'),(300,3000,'string 3'),(NULL,NULL,NULL)", hash[0])
        end
      end
    end
  end
end
