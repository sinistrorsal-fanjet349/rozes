/**
 * Node.js Tests for String & Boolean Column Access (Priority 4 - Milestone 1.1.0)
 *
 * Tests: String column access, Boolean column access, Type detection
 */

const { Rozes } = require('../../../dist/index.js');

async function main() {
  console.log('🧪 Testing String & Boolean Column Access (Priority 4)\n');

  // Initialize Rozes
  const rozes = await Rozes.init();
  console.log('✅ Rozes initialized (version:', rozes.version, ')\n');

  let passedTests = 0;
  let totalTests = 0;

  function test(name, fn) {
    totalTests++;
    try {
      fn();
      console.log(`✅ ${name}`);
      passedTests++;
    } catch (err) {
      console.error(`❌ ${name}`);
      console.error('   Error:', err.message);
      if (err.stack) {
        console.error('   Stack:', err.stack.split('\n').slice(1, 3).join('\n'));
      }
    }
  }

  // ============================================================================
  // Test 1: String Column Access
  // ============================================================================

  const csvWithStrings = `name,age,city
Alice,30,New York
Bob,25,San Francisco
Charlie,35,Seattle
Diana,28,Boston`;

  const df1 = rozes.DataFrame.fromCSV(csvWithStrings);

  test('String column returns array of strings', () => {
    const names = df1.column('name');
    if (!names) throw new Error('Name column not found');
    if (!Array.isArray(names)) {
      throw new Error(`Expected Array, got ${names.constructor.name}`);
    }
    if (names.length !== 4) {
      throw new Error(`Expected 4 values, got ${names.length}`);
    }
    if (names[0] !== 'Alice') throw new Error(`Expected 'Alice', got '${names[0]}'`);
    if (names[1] !== 'Bob') throw new Error(`Expected 'Bob', got '${names[1]}'`);
    if (names[2] !== 'Charlie') throw new Error(`Expected 'Charlie', got '${names[2]}'`);
    if (names[3] !== 'Diana') throw new Error(`Expected 'Diana', got '${names[3]}'`);
  });

  test('String column with empty strings', () => {
    const csv = `name,value
,10
Bob,20
,30`;
    const df = rozes.DataFrame.fromCSV(csv);
    const names = df.column('name');

    if (!names) throw new Error('Name column not found');
    if (names.length !== 3) throw new Error(`Expected 3 values, got ${names.length}`);
    if (names[0] !== '') throw new Error(`Expected empty string, got '${names[0]}'`);
    if (names[1] !== 'Bob') throw new Error(`Expected 'Bob', got '${names[1]}'`);
    if (names[2] !== '') throw new Error(`Expected empty string, got '${names[2]}'`);

    df.free();
  });

  test('String column with special characters', () => {
    const csv = `name,value
"Hello, World",10
"Line1\nLine2",20
"Quote ""test""",30`;
    const df = rozes.DataFrame.fromCSV(csv);
    const names = df.column('name');

    if (!names) throw new Error('Name column not found');
    if (names.length !== 3) throw new Error(`Expected 3 values, got ${names.length}`);
    if (names[0] !== 'Hello, World') throw new Error(`Expected 'Hello, World', got '${names[0]}'`);
    if (names[1] !== 'Line1\nLine2') throw new Error(`Expected newline, got '${names[1]}'`);
    if (names[2] !== 'Quote "test"') throw new Error(`Expected 'Quote "test"', got '${names[2]}'`);

    df.free();
  });

  test('String column with UTF-8 characters', () => {
    const csv = `name,value
Hello 世界,10
Émilie,20
🚀 Rocket,30`;
    const df = rozes.DataFrame.fromCSV(csv);
    const names = df.column('name');

    if (!names) throw new Error('Name column not found');
    if (names.length !== 3) throw new Error(`Expected 3 values, got ${names.length}`);
    if (names[0] !== 'Hello 世界') throw new Error(`Expected 'Hello 世界', got '${names[0]}'`);
    if (names[1] !== 'Émilie') throw new Error(`Expected 'Émilie', got '${names[1]}'`);
    if (names[2] !== '🚀 Rocket') throw new Error(`Expected '🚀 Rocket', got '${names[2]}'`);

    df.free();
  });

  test('Multiple string columns', () => {
    const csv = `first,last,city
Alice,Smith,NYC
Bob,Jones,LA`;
    const df = rozes.DataFrame.fromCSV(csv);

    const first = df.column('first');
    const last = df.column('last');
    const city = df.column('city');

    if (!first || !last || !city) throw new Error('Columns not found');
    if (first[0] !== 'Alice') throw new Error(`Expected 'Alice', got '${first[0]}'`);
    if (last[0] !== 'Smith') throw new Error(`Expected 'Smith', got '${last[0]}'`);
    if (city[0] !== 'NYC') throw new Error(`Expected 'NYC', got '${city[0]}'`);

    df.free();
  });

  // ============================================================================
  // Test 2: Boolean Column Access
  // ============================================================================

  const csvWithBool = `name,age,active
Alice,30,true
Bob,25,false
Charlie,35,true`;

  const df2 = rozes.DataFrame.fromCSV(csvWithBool);

  test('Boolean column returns Uint8Array', () => {
    const active = df2.column('active');
    if (!active) throw new Error('Active column not found');
    if (!(active instanceof Uint8Array)) {
      throw new Error(`Expected Uint8Array, got ${active.constructor.name}`);
    }
    if (active.length !== 3) {
      throw new Error(`Expected 3 values, got ${active.length}`);
    }
    if (active[0] !== 1) throw new Error(`Expected 1 (true), got ${active[0]}`);
    if (active[1] !== 0) throw new Error(`Expected 0 (false), got ${active[1]}`);
    if (active[2] !== 1) throw new Error(`Expected 1 (true), got ${active[2]}`);
  });

  test('Boolean column with all true', () => {
    const csv = `val,active
10,true
20,true
30,true`;
    const df = rozes.DataFrame.fromCSV(csv);
    const active = df.column('active');

    if (!active) throw new Error('Active column not found');
    if (active.length !== 3) throw new Error(`Expected 3 values, got ${active.length}`);
    if (active[0] !== 1 || active[1] !== 1 || active[2] !== 1) {
      throw new Error('Expected all 1s (true)');
    }

    df.free();
  });

  test('Boolean column with all false', () => {
    const csv = `val,active
10,false
20,false
30,false`;
    const df = rozes.DataFrame.fromCSV(csv);
    const active = df.column('active');

    if (!active) throw new Error('Active column not found');
    if (active.length !== 3) throw new Error(`Expected 3 values, got ${active.length}`);
    if (active[0] !== 0 || active[1] !== 0 || active[2] !== 0) {
      throw new Error('Expected all 0s (false)');
    }

    df.free();
  });

  test('Boolean column with case variations', () => {
    const csv = `val,active
10,TRUE
20,False
30,true`;
    const df = rozes.DataFrame.fromCSV(csv);
    const active = df.column('active');

    if (!active) throw new Error('Active column not found');
    if (active.length !== 3) throw new Error(`Expected 3 values, got ${active.length}`);
    if (active[0] !== 1) throw new Error(`Expected 1 (TRUE), got ${active[0]}`);
    if (active[1] !== 0) throw new Error(`Expected 0 (False), got ${active[1]}`);
    if (active[2] !== 1) throw new Error(`Expected 1 (true), got ${active[2]}`);

    df.free();
  });

  // ============================================================================
  // Test 3: Mixed Column Types
  // ============================================================================

  test('DataFrame with all column types', () => {
    const csv = `name,age,score,active
Alice,30,95.5,true
Bob,25,87.3,false`;
    const df = rozes.DataFrame.fromCSV(csv);

    // String column
    const names = df.column('name');
    if (!Array.isArray(names)) throw new Error('Name should be string array');
    if (names[0] !== 'Alice') throw new Error(`Expected 'Alice', got '${names[0]}'`);

    // Int/Float column
    const ages = df.column('age');
    if (!ages || !(ages instanceof BigInt64Array || ages instanceof Float64Array)) {
      throw new Error('Age should be numeric array');
    }

    // Float column
    const scores = df.column('score');
    if (!scores || !(scores instanceof Float64Array)) {
      throw new Error('Score should be Float64Array');
    }

    // Boolean column
    const active = df.column('active');
    if (!(active instanceof Uint8Array)) {
      throw new Error('Active should be Uint8Array');
    }

    df.free();
  });

  test('Column type detection with similar values', () => {
    const csv = `int_col,float_col,str_col,bool_col
1,1.0,one,true
2,2.0,two,false
3,3.0,three,true`;
    const df = rozes.DataFrame.fromCSV(csv);

    const intCol = df.column('int_col');
    const floatCol = df.column('float_col');
    const strCol = df.column('str_col');
    const boolCol = df.column('bool_col');

    if (!(intCol instanceof BigInt64Array || intCol instanceof Float64Array)) {
      throw new Error('int_col should be numeric');
    }
    if (!(floatCol instanceof Float64Array)) {
      throw new Error('float_col should be Float64Array');
    }
    if (!Array.isArray(strCol)) {
      throw new Error('str_col should be string array');
    }
    if (!(boolCol instanceof Uint8Array)) {
      throw new Error('bool_col should be Uint8Array');
    }

    df.free();
  });

  // ============================================================================
  // Test 4: Error Cases
  // ============================================================================

  test('Non-existent column returns null', () => {
    const csv = `name,age
Alice,30`;
    const df = rozes.DataFrame.fromCSV(csv);

    const result = df.column('nonexistent');
    if (result !== null) {
      throw new Error(`Expected null, got ${result}`);
    }

    df.free();
  });

  test('Empty column name returns null', () => {
    const csv = `name,age
Alice,30`;
    const df = rozes.DataFrame.fromCSV(csv);

    const result = df.column('');
    if (result !== null) {
      throw new Error(`Expected null, got ${result}`);
    }

    df.free();
  });

  // Clean up
  df1.free();
  df2.free();

  // ============================================================================
  // Results
  // ============================================================================

  console.log(`\n${'='.repeat(60)}`);
  console.log(`Results: ${passedTests}/${totalTests} tests passed`);
  console.log('='.repeat(60));

  if (passedTests === totalTests) {
    console.log('\n✅ All tests passed!');
    process.exit(0);
  } else {
    console.log(`\n❌ ${totalTests - passedTests} test(s) failed`);
    process.exit(1);
  }
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
